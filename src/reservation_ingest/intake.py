import hashlib
import logging
import time
from dataclasses import dataclass
from typing import cast
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import case, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from reservation_ingest.catalog import DEFAULT_INTEGRATION_NAME, seeded_tenant
from reservation_ingest.config import BACKLOG_SHARDS, CHUNK_ROWS, Settings
from reservation_ingest.db import Sessions
from reservation_ingest.models import (
    BacklogBudget,
    ImportReceipt,
    Integration,
    ItemResult,
    Job,
    Outbox,
    TenantBacklog,
    TenantUsage,
)
from reservation_ingest.schemas import (
    IntakeState,
    ItemState,
    Lane,
    ReceiptStatus,
    ResultItem,
    canonical_json,
)

logger = logging.getLogger(__name__)


class IntakeError(Exception):
    def __init__(self, status: int, code: str, retry_after: int | None = None) -> None:
        super().__init__(code)
        self.status = status
        self.code = code
        self.retry_after = retry_after


@dataclass(frozen=True, slots=True)
class Partner:
    tenant_id: UUID
    integration_id: UUID


async def resolve_integration(
    sessions: Sessions, integration_id: UUID | None
) -> Partner:
    query = select(Integration.tenant_id, Integration.id)
    if integration_id is None:
        query = query.where(
            Integration.tenant_id == seeded_tenant(1),
            Integration.name == DEFAULT_INTEGRATION_NAME,
        )
    else:
        query = query.where(Integration.id == integration_id)
    async with sessions() as session:
        row = (await session.execute(query)).one_or_none()
    if row is None:
        error = (
            "DEFAULT_INTEGRATION_NOT_SEEDED"
            if integration_id is None
            else "UNKNOWN_INTEGRATION"
        )
        raise IntakeError(422, error)
    return Partner(row.tenant_id, row.id)


async def change_backlog(
    session: AsyncSession, lane: str, shard: int, delta: int, cap: int | None = None
) -> None:
    stmt = (
        update(BacklogBudget)
        .where(BacklogBudget.lane == lane, BacklogBudget.shard == shard)
        .values(pending_rows=BacklogBudget.pending_rows + delta)
        .returning(BacklogBudget.shard)
    )
    if cap is not None:
        stmt = stmt.where(BacklogBudget.pending_rows + delta <= cap)
    if await session.scalar(stmt) is None:
        if cap is not None:
            raise IntakeError(503, "BACKLOG_FULL", 5)
        raise RuntimeError(f"missing backlog budget for {lane}/{shard}")


async def change_tenant_backlog(
    session: AsyncSession,
    tenant_id: UUID,
    lane: str,
    delta: int,
    cap: int | None = None,
) -> None:
    if cap is None:
        remaining = await session.scalar(
            update(TenantBacklog)
            .where(TenantBacklog.tenant_id == tenant_id, TenantBacklog.lane == lane)
            .values(pending_rows=TenantBacklog.pending_rows + delta)
            .returning(TenantBacklog.pending_rows)
        )
        if remaining is None:
            raise RuntimeError("missing tenant backlog budget")
        return

    if delta > cap:
        raise IntakeError(429, "TENANT_BACKLOG_FULL", 5)
    reserved = await session.scalar(
        insert(TenantBacklog)
        .values(tenant_id=tenant_id, lane=lane, pending_rows=delta)
        .on_conflict_do_update(
            index_elements=[TenantBacklog.tenant_id, TenantBacklog.lane],
            set_={"pending_rows": TenantBacklog.pending_rows + delta},
            where=TenantBacklog.pending_rows + delta <= cap,
        )
        .returning(TenantBacklog.pending_rows)
    )
    if reserved is None:
        raise IntakeError(429, "TENANT_BACKLOG_FULL", 5)


async def reserve_backlog(
    session: AsyncSession,
    settings: Settings,
    tenant_id: UUID,
    lane: str,
    shard: int,
    rows: int,
    *,
    choose_shard: bool = False,
) -> int:
    if lane == "bulk":
        tenant_cap, total = (
            settings.tenant_bulk_backlog_rows,
            settings.bulk_backlog_rows,
        )
    else:
        tenant_cap, total = (
            settings.tenant_single_backlog_rows,
            settings.single_backlog_rows,
        )
    base, remainder = divmod(total, BACKLOG_SHARDS)
    await change_tenant_backlog(session, tenant_id, lane, rows, tenant_cap)
    if not choose_shard:
        await change_backlog(session, lane, shard, rows, base + int(shard < remainder))
        return shard

    cap = base + case((BacklogBudget.shard < remainder, 1), else_=0)
    available = (
        select(BacklogBudget.shard)
        .where(BacklogBudget.lane == lane, BacklogBudget.pending_rows + rows <= cap)
        .order_by(BacklogBudget.pending_rows, BacklogBudget.shard != shard)
        .limit(1)
        .with_for_update(skip_locked=True)
        .cte("available_budget")
    )
    selected = await session.scalar(
        update(BacklogBudget)
        .where(BacklogBudget.lane == lane, BacklogBudget.shard == available.c.shard)
        .values(pending_rows=BacklogBudget.pending_rows + rows)
        .returning(BacklogBudget.shard)
    )
    if selected is None:
        raise IntakeError(503, "BACKLOG_FULL", 5)
    return selected


async def release_backlog(
    session: AsyncSession, tenant_id: UUID, lane: str, shard: int, rows: int
) -> None:
    await change_tenant_backlog(session, tenant_id, lane, -rows)
    await change_backlog(session, lane, shard, -rows)


async def charge_tenant(
    session: AsyncSession, tenant_id: UUID, lane: Lane, rows: int, limit: int
) -> None:
    now = int(time.time())
    if rows > limit:
        raise IntakeError(429, "TENANT_RATE_LIMIT", 60 - now % 60)
    stmt = (
        insert(TenantUsage)
        .values(tenant_id=tenant_id, lane=lane, minute=now // 60, rows=rows)
        .on_conflict_do_update(
            index_elements=[
                TenantUsage.tenant_id,
                TenantUsage.lane,
                TenantUsage.minute,
            ],
            set_={"rows": TenantUsage.rows + rows},
            where=TenantUsage.rows + rows <= limit,
        )
        .returning(TenantUsage.rows)
    )
    if await session.scalar(stmt) is None:
        raise IntakeError(429, "TENANT_RATE_LIMIT", 60 - now % 60)


async def submit(
    sessions: Sessions,
    settings: Settings,
    partner: Partner,
    lane: Lane,
    key: str,
    rows: list[JsonValue],
) -> tuple[UUID, bool]:
    receipt_id = uuid4()
    shard = receipt_id.int % BACKLOG_SHARDS
    try:
        payload = canonical_json(rows)
        digest = hashlib.sha256(payload.encode()).hexdigest()
    except ValueError as exc:
        raise IntakeError(422, "INVALID_JSON_VALUE") from exc

    async with sessions.begin() as session:
        new_id = await session.scalar(
            insert(ImportReceipt)
            .values(
                id=receipt_id,
                tenant_id=partner.tenant_id,
                integration_id=partner.integration_id,
                kind=lane,
                idempotency_key=key,
                request_hash=digest,
                payload=payload,
                row_count=len(rows),
                budget_shard=shard,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    ImportReceipt.integration_id,
                    ImportReceipt.kind,
                    ImportReceipt.idempotency_key,
                ]
            )
            .returning(ImportReceipt.id)
        )
        if new_id is None:
            previous = (
                await session.execute(
                    select(ImportReceipt.id, ImportReceipt.request_hash).where(
                        ImportReceipt.integration_id == partner.integration_id,
                        ImportReceipt.kind == lane,
                        ImportReceipt.idempotency_key == key,
                    )
                )
            ).one()
            if previous.request_hash != digest:
                raise IntakeError(409, "IDEMPOTENCY_KEY_CONFLICT")
            return (previous.id, False)

        jobs = [
            {
                "id": uuid4(),
                "import_id": receipt_id,
                "start_index": start,
                "row_count": min(CHUNK_ROWS, len(rows) - start),
            }
            for start in range(0, len(rows), CHUNK_ROWS)
        ]
        await session.execute(insert(Job), jobs)
        await session.execute(
            insert(Outbox),
            [
                {"id": uuid4(), "job_id": job["id"], "generation": 1, "lane": lane}
                for job in jobs
            ],
        )
        rate_limit = (
            settings.tenant_bulk_rows_per_minute
            if lane == "bulk"
            else settings.tenant_single_rows_per_minute
        )
        await charge_tenant(session, partner.tenant_id, lane, len(rows), rate_limit)
        selected_shard = await reserve_backlog(
            session,
            settings,
            partner.tenant_id,
            lane,
            shard,
            len(rows),
            choose_shard=True,
        )
        if selected_shard != shard:
            await session.execute(
                update(ImportReceipt)
                .where(ImportReceipt.id == receipt_id)
                .values(budget_shard=selected_shard)
            )

    logger.info("receipt accepted id=%s lane=%s rows=%d", receipt_id, lane, len(rows))
    return (receipt_id, True)


async def receipt_status(
    sessions: Sessions,
    partner: Partner,
    lane: Lane,
    receipt_id: UUID,
    after: int,
    limit: int,
) -> ReceiptStatus:
    async with sessions() as session:
        await session.connection(
            execution_options={"isolation_level": "REPEATABLE READ"}
        )
        total = await session.scalar(
            select(ImportReceipt.row_count).where(
                ImportReceipt.id == receipt_id,
                ImportReceipt.integration_id == partner.integration_id,
                ImportReceipt.kind == lane,
            )
        )
        if total is None:
            raise IntakeError(404, "RECEIPT_NOT_FOUND")
        jobs = (
            await session.execute(
                select(Job.succeeded, Job.rejected, Job.failed).where(
                    Job.import_id == receipt_id
                )
            )
        ).all()
        start = min(after + 1, total)
        end = min(start + limit, total)
        rows = (
            await session.scalars(
                select(ItemResult)
                .where(
                    ItemResult.import_id == receipt_id,
                    ItemResult.row_index >= start,
                    ItemResult.row_index < end,
                )
                .order_by(ItemResult.row_index)
            )
        ).all()

    succeeded = sum(job.succeeded for job in jobs)
    rejected = sum(job.rejected for job in jobs)
    failed = sum(job.failed for job in jobs)
    pending = total - succeeded - rejected - failed
    state: IntakeState
    if pending == total:
        state = "accepted"
    elif pending:
        state = "processing"
    elif failed == total:
        state = "failed"
    elif rejected or failed:
        state = "completed_with_errors"
    else:
        state = "completed"
    outcomes = {row.row_index: row for row in rows}
    items = []
    for index in range(start, end):
        row = outcomes.get(index)
        items.append(
            ResultItem(
                row_index=index,
                state=cast(ItemState, row.state) if row else "pending",
                error_code=row.error_code if row else None,
                reservation_id=row.reservation_id if row else None,
            )
        )
    return ReceiptStatus(
        id=receipt_id,
        state=state,
        total=total,
        pending=pending,
        succeeded=succeeded,
        rejected=rejected,
        failed=failed,
        items=items,
        next_after=end - 1 if end < total else None,
    )
