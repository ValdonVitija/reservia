import time
from datetime import UTC, datetime, timedelta

import aio_pika
from sqlalchemy import delete, func, select, update

from reservation_ingest.config import Settings
from reservation_ingest.db import Sessions
from reservation_ingest.models import (
    BacklogBudget,
    ImportReceipt,
    Job,
    Outbox,
    TenantUsage,
)


async def stats(sessions: Sessions, settings: Settings) -> dict[str, object]:
    async with sessions() as session:
        pending = dict(
            (
                await session.execute(
                    select(
                        BacklogBudget.lane, func.sum(BacklogBudget.pending_rows)
                    ).group_by(BacklogBudget.lane)
                )
            )
            .tuples()
            .all()
        )
        stale = (
            (
                await session.execute(
                    select(Job.id)
                    .join(
                        Outbox,
                        (Outbox.job_id == Job.id)
                        & (Outbox.generation == Job.generation),
                    )
                    .where(
                        Job.state == "pending",
                        Outbox.sent_at
                        < datetime.now(UTC)
                        - timedelta(seconds=settings.reconcile_after_seconds),
                    )
                    .order_by(Outbox.sent_at, Job.id)
                    .limit(100)
                )
            )
            .scalars()
            .all()
        )
        oldest_pending = await session.scalar(
            select(func.min(ImportReceipt.created_at))
            .join(Job, Job.import_id == ImportReceipt.id)
            .where(Job.state == "pending")
        )
        oldest_unpublished = await session.scalar(
            select(func.min(Outbox.available_at)).where(Outbox.sent_at.is_(None))
        )
        failed_jobs = (
            await session.scalars(
                select(Job.id)
                .where(Job.state == "failed")
                .order_by(Job.updated_at.desc(), Job.id)
                .limit(100)
            )
        ).all()

    queues: dict[str, int | None] = {}
    try:
        async with await aio_pika.connect(
            settings.rabbitmq_url, timeout=3
        ) as connection:
            for lane in ("bulk", "single", "dead"):
                async with connection.channel() as channel:
                    queue = await channel.declare_queue(
                        f"reservations.{lane}", passive=True, timeout=3
                    )
                    queues[lane] = queue.declaration_result.message_count
    except (aio_pika.AMQPException, OSError, TimeoutError):
        pass

    now = datetime.now(UTC)
    return {
        "pending_rows": {lane: int(rows) for lane, rows in pending.items()},
        "rabbitmq_ready_messages": queues,
        "overdue_job_ids": [str(job_id) for job_id in stale],
        "failed_job_ids": [str(job_id) for job_id in failed_jobs],
        "oldest_pending_seconds": (
            int((now - oldest_pending).total_seconds()) if oldest_pending else None
        ),
        "oldest_unpublished_seconds": (
            max(0, int((now - oldest_unpublished).total_seconds()))
            if oldest_unpublished
            else None
        ),
    }


async def prune(sessions: Sessions, retention_days: int) -> int:
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    async with sessions.begin() as session:
        removable = (
            select(ImportReceipt.id)
            .where(
                ImportReceipt.payload.is_not(None),
                ImportReceipt.created_at < cutoff,
                ~select(Job.id)
                .where(
                    Job.import_id == ImportReceipt.id,
                    (Job.state == "pending") | (Job.updated_at >= cutoff),
                )
                .exists(),
            )
            .order_by(ImportReceipt.created_at)
            .limit(500)
            .with_for_update(skip_locked=True)
        )
        removed = (
            await session.scalars(
                update(ImportReceipt)
                .where(ImportReceipt.id.in_(removable))
                .values(payload=None)
                .returning(ImportReceipt.id)
            )
        ).all()
        await session.execute(
            delete(TenantUsage).where(
                TenantUsage.minute < int(time.time()) // 60 - 1440
            )
        )
    return len(removed)
