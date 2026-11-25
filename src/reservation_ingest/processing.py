import hashlib
import json
import logging
import random
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4, uuid5

from pydantic import ValidationError
from sqlalchemy import and_, delete, or_, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from reservation_ingest.config import MAX_JOB_ATTEMPTS, Settings
from reservation_ingest.db import Sessions
from reservation_ingest.intake import release_backlog, reserve_backlog
from reservation_ingest.models import (
    Housing,
    ImportReceipt,
    ItemResult,
    Job,
    Outbox,
    Quarantine,
    Reservation,
    Room,
)
from reservation_ingest.schemas import DispatchMessage, ReservationInput, canonical_hash

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Candidate:
    index: int
    data: ReservationInput
    values: dict[str, Any]


async def lock_current_job(
    session: AsyncSession, message: DispatchMessage
) -> Job | None:
    job = await session.scalar(
        select(Job)
        .join(
            Outbox,
            and_(Outbox.job_id == Job.id, Outbox.generation == Job.generation),
        )
        .where(
            Job.id == message.job_id,
            Job.state == "pending",
            Job.generation == message.generation,
            Outbox.id == message.event_id,
        )
        .with_for_update(of=Job)
    )
    return job


def result(
    receipt_id: UUID,
    index: int,
    state: str,
    error_code: str | None = None,
    reservation_id: UUID | None = None,
) -> dict[str, Any]:
    return {
        "import_id": receipt_id,
        "row_index": index,
        "state": state,
        "error_code": error_code,
        "reservation_id": reservation_id,
    }


async def validate_chunk(
    session: AsyncSession, receipt: ImportReceipt, job: Job
) -> tuple[list[Candidate], list[dict[str, Any]]]:
    if receipt.payload is None:
        raise RuntimeError("pending job has no staged payload")
    rows = json.loads(receipt.payload)[
        job.start_index : job.start_index + job.row_count
    ]
    if len(rows) != job.row_count:
        raise RuntimeError("staged payload does not match the chunk range")
    parsed: list[tuple[int, ReservationInput]] = []
    results = []
    for index, row in enumerate(rows, start=job.start_index):
        try:
            parsed.append((index, ReservationInput.model_validate(row)))
        except ValidationError:
            results.append(result(receipt.id, index, "rejected", "INVALID_RESERVATION"))

    housing_ids = {data.housing for _, data in parsed}
    authorized_housings = (
        set(
            await session.scalars(
                select(Housing.id).where(
                    Housing.id.in_(housing_ids), Housing.tenant_id == receipt.tenant_id
                )
            )
        )
        if housing_ids
        else set()
    )
    room_ids = {room_id for _, data in parsed for room_id in data.room_ids}
    rooms = (
        dict(
            (
                await session.execute(
                    select(Room.id, Room.housing_id).where(
                        Room.id.in_(room_ids), Room.housing_id.in_(authorized_housings)
                    )
                )
            )
            .tuples()
            .all()
        )
        if room_ids and authorized_housings
        else {}
    )

    candidates = []
    for index, data in parsed:
        if data.housing not in authorized_housings:
            results.append(result(receipt.id, index, "rejected", "UNKNOWN_HOUSING"))
        elif any(rooms.get(room_id) != data.housing for room_id in data.room_ids):
            results.append(result(receipt.id, index, "rejected", "ROOM_NOT_IN_HOUSING"))
        else:
            payload = data.model_dump(mode="json")
            candidates.append(
                Candidate(
                    index,
                    data,
                    {
                        "id": uuid5(receipt.id, str(index)),
                        "integration_id": receipt.integration_id,
                        "housing_id": data.housing,
                        "external_id": data.external_id,
                        "check_in_date": data.check_in_date,
                        "check_out_date": data.check_out_date,
                        "guest_count": data.guest_group.known_number_of_guests,
                        "payload": payload,
                        "payload_hash": canonical_hash(payload),
                    },
                )
            )
    return candidates, results


async def write_reservations(
    session: AsyncSession, receipt: ImportReceipt, candidates: Sequence[Candidate]
) -> list[dict[str, Any]]:
    if not candidates:
        return []
    # Keep lock acquisition order stable when concurrent imports have overlapping IDs.
    ordered = sorted(
        candidates,
        key=lambda item: (item.data.housing, item.data.external_id or "", item.index),
    )
    await session.execute(
        insert(Reservation)
        .values([candidate.values for candidate in ordered])
        .on_conflict_do_nothing()
    )

    external_keys = {
        (item.data.housing, item.data.external_id)
        for item in candidates
        if item.data.external_id is not None
    }
    records = (
        await session.scalars(
            select(Reservation).where(
                Reservation.integration_id == receipt.integration_id,
                or_(
                    Reservation.id.in_([item.values["id"] for item in candidates]),
                    tuple_(Reservation.housing_id, Reservation.external_id).in_(
                        external_keys
                    ),
                ),
            )
        )
    ).all()
    by_id = {record.id: record for record in records}
    by_external = {
        (record.housing_id, record.external_id): record
        for record in records
        if record.external_id is not None
    }
    results = []
    for item in candidates:
        record = by_id.get(item.values["id"])
        if record is None and item.data.external_id is not None:
            record = by_external.get((item.data.housing, item.data.external_id))
        if record is None:
            raise RuntimeError("reservation missing after insertion")
        if record.payload_hash == item.values["payload_hash"]:
            results.append(
                result(receipt.id, item.index, "completed", reservation_id=record.id)
            )
        else:
            results.append(
                result(receipt.id, item.index, "rejected", "EXTERNAL_ID_CONFLICT")
            )
    return results


async def process_job(sessions: Sessions, message: DispatchMessage) -> None:
    async with sessions.begin() as session:
        job = await lock_current_job(session, message)
        if job is None:
            return
        receipt = await session.get_one(ImportReceipt, job.import_id)
        candidates, results = await validate_chunk(session, receipt, job)
        results.extend(await write_reservations(session, receipt, candidates))
        await session.execute(insert(ItemResult), results)
        job.succeeded = sum(row["state"] == "completed" for row in results)
        job.rejected = job.row_count - job.succeeded
        job.state = "completed"
        job.updated_at = datetime.now(UTC)
        await release_backlog(
            session,
            receipt.tenant_id,
            receipt.kind,
            receipt.budget_shard,
            job.row_count,
        )
    logger.info(
        "job completed job=%s receipt=%s accepted=%d rejected=%d",
        job.id,
        job.import_id,
        job.succeeded,
        job.rejected,
    )


async def mark_failed(
    session: AsyncSession, job: Job, receipt: ImportReceipt, reason: str
) -> None:
    await session.execute(
        insert(ItemResult),
        [
            result(receipt.id, index, "failed", reason)
            for index in range(job.start_index, job.start_index + job.row_count)
        ],
    )
    job.state = "failed"
    job.failed = job.row_count
    job.updated_at = datetime.now(UTC)
    await release_backlog(
        session, receipt.tenant_id, receipt.kind, receipt.budget_shard, job.row_count
    )
    logger.warning(
        "job terminal failure job=%s receipt=%s reason=%s", job.id, receipt.id, reason
    )


def enqueue(session: AsyncSession, job: Job, lane: str, delay: float = 0) -> None:
    now = datetime.now(UTC)
    job.generation += 1
    job.updated_at = now
    session.add(
        Outbox(
            id=uuid4(),
            job_id=job.id,
            generation=job.generation,
            lane=lane,
            available_at=now + timedelta(seconds=delay),
        )
    )


def retry_delay(base: float, maximum: float, attempt: int) -> float:
    ceiling = min(maximum, base * 2 ** min(attempt - 1, 20))
    return random.uniform(ceiling / 2, ceiling)


async def retry_job(
    sessions: Sessions, settings: Settings, message: DispatchMessage
) -> None:
    async with sessions.begin() as session:
        job = await lock_current_job(session, message)
        if job is None:
            return
        receipt = await session.get_one(ImportReceipt, job.import_id)
        job.attempts += 1
        if job.attempts >= MAX_JOB_ATTEMPTS:
            await mark_failed(session, job, receipt, "PROCESSING_RETRIES_EXHAUSTED")
        else:
            delay = retry_delay(
                settings.retry_base_seconds, settings.retry_max_seconds, job.attempts
            )
            enqueue(session, job, receipt.kind, delay)
            logger.warning(
                "job retry scheduled job=%s attempts=%d", job.id, job.attempts
            )


async def quarantine_message(sessions: Sessions, body: bytes) -> None:
    try:
        message = DispatchMessage.model_validate_json(body)
    except ValidationError:
        message = None
    async with sessions.begin() as session:
        if (
            message is not None
            and (job := await lock_current_job(session, message)) is not None
        ):
            receipt = await session.get_one(ImportReceipt, job.import_id)
            await mark_failed(session, job, receipt, "BROKER_DELIVERY_EXHAUSTED")
        await session.execute(
            insert(Quarantine)
            .values(
                digest=hashlib.sha256(body).hexdigest(),
                job_id=message.job_id if message else None,
                error_code="BROKER_DEAD_LETTER" if message else "INVALID_MESSAGE",
            )
            .on_conflict_do_nothing()
        )


async def replay_job(sessions: Sessions, settings: Settings, job_id: UUID) -> UUID:
    async with sessions.begin() as session:
        receipt_id = await session.scalar(select(Job.import_id).where(Job.id == job_id))
        if receipt_id is None:
            raise ValueError("unknown job")
        receipt = await session.get_one(ImportReceipt, receipt_id, with_for_update=True)
        job = await session.get_one(Job, job_id, with_for_update=True)
        if job.state == "completed":
            raise ValueError("use a failed or an overdue pending job ID")
        if receipt.payload is None:
            raise ValueError("the replay window for this import has expired")
        if job.state == "pending":
            sent_at = await session.scalar(
                select(Outbox.sent_at).where(
                    Outbox.job_id == job.id, Outbox.generation == job.generation
                )
            )
            cutoff = datetime.now(UTC) - timedelta(
                seconds=settings.reconcile_after_seconds
            )
            if sent_at is None or sent_at >= cutoff:
                raise ValueError("the pending job has not reached the replay age")
        else:
            await reserve_backlog(
                session,
                settings,
                receipt.tenant_id,
                receipt.kind,
                receipt.budget_shard,
                job.row_count,
            )
            await session.execute(
                delete(ItemResult).where(
                    ItemResult.import_id == receipt.id,
                    ItemResult.row_index >= job.start_index,
                    ItemResult.row_index < job.start_index + job.row_count,
                )
            )
        job.state = "pending"
        job.failed = 0
        job.attempts = 0
        enqueue(session, job, receipt.kind)
    return receipt.id
