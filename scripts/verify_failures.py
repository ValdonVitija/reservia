import argparse
import asyncio
import hashlib
import shlex
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import aio_pika
import httpx
from sqlalchemy import func, select, update

from reservation_ingest.broker import declare_topology
from reservation_ingest.catalog import seed, seeded_housing, seeded_tenant
from reservation_ingest.config import MAX_JOB_ATTEMPTS, Settings
from reservation_ingest.db import Sessions, create_database
from reservation_ingest.models import Job, Outbox, Quarantine, Reservation
from reservation_ingest.processing import process_job, replay_job, retry_job
from reservation_ingest.relay import claim_events, publish_event
from reservation_ingest.schemas import DispatchMessage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Interrupt services in an isolated test stack"
    )
    parser.add_argument("--compose-command", default="docker compose")
    parser.add_argument("--project", required=True)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    return parser.parse_args()


async def compose(args: argparse.Namespace, *options: str) -> None:
    process = await asyncio.create_subprocess_exec(
        *shlex.split(args.compose_command),
        "-p",
        args.project,
        *options,
        stdout=asyncio.subprocess.DEVNULL,
    )
    if await process.wait() != 0:
        raise RuntimeError(f"Compose failed for {options}")


def booking(external_id: str) -> dict[str, Any]:
    return {
        "reservations": [
            {
                "housing": str(seeded_housing(seeded_tenant(1), 1)),
                "external_id": external_id,
                "check_in_date": "2027-07-12",
                "check_out_date": "2027-07-15",
                "guest_group": {"known_number_of_guests": 2},
            }
        ],
    }


async def submit(
    client: httpx.AsyncClient,
    row: dict[str, Any],
    key: str,
) -> httpx.Response:
    return await client.post(
        "/v1/reservation-imports", json=row, headers={"Idempotency-Key": key}
    )


async def completed(client: httpx.AsyncClient, receipt: str) -> None:
    async with asyncio.timeout(45):
        while True:
            response = await client.get(receipt)
            response.raise_for_status()
            if response.json()["pending"] == 0:
                assert response.json()["succeeded"] == 1, response.text
                return
            await asyncio.sleep(0.2)


async def check_outages(args: argparse.Namespace, client: httpx.AsyncClient) -> None:
    row = booking(f"fault-broker-{uuid4()}")
    accepted_key = uuid4().hex
    await compose(args, "stop", "rabbitmq")
    try:
        accepted = await submit(client, row, accepted_key)
        assert accepted.status_code == 202, accepted.text
        status = await client.get(accepted.headers["Location"])
        assert status.json()["pending"] == 1, status.text
    finally:
        await compose(args, "start", "rabbitmq")
    await completed(client, accepted.headers["Location"])
    print("Broker outage preserved the receipt and drained after restart.", flush=True)

    next_row = booking(f"fault-postgres-{uuid4()}")
    next_key = uuid4().hex
    await compose(args, "stop", "postgres")
    try:
        unavailable = await submit(client, next_row, next_key)
        assert unavailable.status_code == 503, unavailable.text
        assert "Retry-After" in unavailable.headers
    finally:
        await compose(args, "start", "postgres")

    async with asyncio.timeout(30):
        while True:
            probe = await client.get("/health/ready")
            if probe.status_code == 200:
                break
            await asyncio.sleep(0.25)
    retry = await submit(client, row, accepted_key)
    assert retry.status_code == 200
    assert retry.headers["Location"] == accepted.headers["Location"]
    fresh = await submit(client, next_row, next_key)
    assert fresh.status_code == 202, fresh.text
    await completed(client, fresh.headers["Location"])
    print(
        "PostgreSQL outage returned 503 and preserved safe retry semantics.", flush=True
    )


async def check_crash_windows(
    args: argparse.Namespace,
    client: httpx.AsyncClient,
    sessions: Sessions,
    settings: Settings,
) -> None:
    external_id = f"fault-commit-{uuid4()}"
    await compose(args, "stop", "relay", "worker-bulk")
    try:
        response = await submit(client, booking(external_id), uuid4().hex)
        assert response.status_code == 202, response.text
        receipt_id = UUID(response.json()["id"])
        async with sessions() as session:
            job_id = await session.scalar(
                select(Job.id).where(Job.import_id == receipt_id)
            )
        assert job_id is not None

        events = await claim_events(sessions, "bulk", uuid4())
        assert len(events) == 1 and events[0].message.job_id == job_id
        event = events[0]

        async with await aio_pika.connect(
            settings.rabbitmq_url, timeout=5
        ) as connection:
            async with connection.channel(on_return_raises=True) as channel:
                topology = await declare_topology(channel)
                assert await publish_event(topology.exchange, "bulk", event)
            # The publisher disconnected before recording the confirmation.
            async with connection.channel() as channel:
                topology = await declare_topology(channel)
                delivery = await topology.bulk.get(timeout=5)
                assert delivery is not None
                message = DispatchMessage.model_validate_json(delivery.body)
                assert message.job_id == job_id
                await process_job(sessions, message)
                # Disconnect after commit to force redelivery.

        async with sessions.begin() as session:
            published = await session.get_one(Outbox, event.message.event_id)
            assert published.sent_at is None
            await session.execute(
                update(Outbox)
                .where(Outbox.id == event.message.event_id)
                .values(lease_until=datetime.now(UTC) - timedelta(seconds=1))
            )
    finally:
        await compose(args, "start", "relay", "worker-bulk")

    await completed(client, response.headers["Location"])
    async with asyncio.timeout(30):
        while True:
            async with sessions() as session:
                sent_at = await session.scalar(
                    select(Outbox.sent_at).where(Outbox.id == event.message.event_id)
                )
            if sent_at:
                break
            await asyncio.sleep(0.2)
    async with sessions() as session:
        count = await session.scalar(
            select(func.count(Reservation.id)).where(
                Reservation.external_id == external_id
            )
        )
        assert count == 1
    print(
        "Relay and worker crash windows recovered with exactly one reservation.",
        flush=True,
    )


async def check_quarantine(sessions: Sessions, settings: Settings) -> None:
    payload = f"malformed-{uuid4()}".encode()
    digest = hashlib.sha256(payload).hexdigest()
    async with await aio_pika.connect(settings.rabbitmq_url, timeout=5) as connection:
        async with connection.channel(on_return_raises=True) as channel:
            topology = await declare_topology(channel)
            await topology.exchange.publish(
                aio_pika.Message(
                    payload, delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key="bulk",
                mandatory=True,
            )
    async with asyncio.timeout(20):
        while True:
            async with sessions() as session:
                quarantine = await session.get(Quarantine, digest)
                if quarantine:
                    assert quarantine.error_code == "INVALID_MESSAGE"
                    break
            await asyncio.sleep(0.2)
    print(
        "A malformed message reached the dead-letter queue and durable quarantine.",
        flush=True,
    )


async def check_replay(
    args: argparse.Namespace,
    client: httpx.AsyncClient,
    sessions: Sessions,
    settings: Settings,
) -> None:
    await compose(args, "stop", "relay", "worker-bulk")
    try:
        response = await submit(client, booking(f"fault-replay-{uuid4()}"), uuid4().hex)
        assert response.status_code == 202, response.text
        receipt_id = UUID(response.json()["id"])
        async with sessions() as session:
            job_id = await session.scalar(
                select(Job.id).where(Job.import_id == receipt_id)
            )
        assert job_id is not None

        for generation in range(1, MAX_JOB_ATTEMPTS + 1):
            async with sessions() as session:
                event_id = await session.scalar(
                    select(Outbox.id).where(
                        Outbox.job_id == job_id, Outbox.generation == generation
                    )
                )
            assert event_id is not None
            await retry_job(
                sessions,
                settings,
                DispatchMessage(
                    job_id=job_id, event_id=event_id, generation=generation
                ),
            )

        status = (await client.get(response.headers["Location"])).json()
        assert status["failed"] == 1 and status["pending"] == 0
        assert status["items"][0]["error_code"] == "PROCESSING_RETRIES_EXHAUSTED"
        assert await replay_job(sessions, settings, job_id) == receipt_id
    finally:
        await compose(args, "start", "relay", "worker-bulk")

    await completed(client, response.headers["Location"])
    print(
        "Application retries stopped at the limit and operator replay recovered.",
        flush=True,
    )


async def run(args: argparse.Namespace) -> None:
    settings = Settings()
    engine, sessions = create_database(settings)
    try:
        await seed(sessions, 1, 1, 0)
        async with httpx.AsyncClient(base_url=args.url, timeout=12) as client:
            await check_outages(args, client)
            await check_crash_windows(args, client, sessions, settings)
            await check_replay(args, client, sessions, settings)
        await check_quarantine(sessions, settings)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
