import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import aio_pika
from aio_pika import abc
from pamqp.commands import Basic
from sqlalchemy import or_, select, update

from reservation_ingest.broker import declare_topology
from reservation_ingest.config import Settings
from reservation_ingest.db import Sessions
from reservation_ingest.models import Outbox
from reservation_ingest.processing import retry_delay
from reservation_ingest.schemas import DispatchMessage, Lane

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class LeasedEvent:
    message: DispatchMessage
    publish_failures: int


async def claim_events(
    sessions: Sessions, lane: Lane, token: UUID
) -> list[LeasedEvent]:
    now = datetime.now(UTC)
    async with sessions.begin() as session:
        ready = (
            select(Outbox.id)
            .where(
                Outbox.lane == lane,
                Outbox.sent_at.is_(None),
                Outbox.available_at <= now,
                or_(Outbox.lease_until.is_(None), Outbox.lease_until <= now),
            )
            .order_by(Outbox.available_at, Outbox.id)
            .limit(32)
            .with_for_update(skip_locked=True)
        )
        rows = (
            await session.execute(
                update(Outbox)
                .where(Outbox.id.in_(ready))
                .values(lease_token=token, lease_until=now + timedelta(seconds=30))
                .returning(
                    Outbox.id, Outbox.job_id, Outbox.generation, Outbox.publish_failures
                )
            )
        ).all()
    return [
        LeasedEvent(
            DispatchMessage(
                event_id=row.id, job_id=row.job_id, generation=row.generation
            ),
            row.publish_failures,
        )
        for row in rows
    ]


async def publish_event(
    exchange: abc.AbstractExchange, lane: Lane, event: LeasedEvent
) -> bool:
    try:
        confirmation = await exchange.publish(
            aio_pika.Message(
                event.message.model_dump_json().encode(),
                message_id=str(event.message.event_id),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=lane,
            mandatory=True,
            timeout=5,
        )
    except (aio_pika.AMQPException, OSError, TimeoutError):
        return False
    return isinstance(confirmation, Basic.Ack)


async def settle_events(
    sessions: Sessions,
    settings: Settings,
    token: UUID,
    settled: list[tuple[LeasedEvent, bool]],
) -> None:
    now = datetime.now(UTC)
    async with sessions.begin() as session:
        confirmed = [event.message.event_id for event, ok in settled if ok]
        if confirmed:
            await session.execute(
                update(Outbox)
                .where(Outbox.id.in_(confirmed), Outbox.lease_token == token)
                .values(sent_at=now, lease_token=None, lease_until=None)
            )
        for event, ok in settled:
            if not ok:
                attempt = event.publish_failures + 1
                delay = retry_delay(
                    settings.retry_base_seconds, settings.retry_max_seconds, attempt
                )
                await session.execute(
                    update(Outbox)
                    .where(
                        Outbox.id == event.message.event_id, Outbox.lease_token == token
                    )
                    .values(
                        publish_failures=attempt,
                        available_at=now + timedelta(seconds=delay),
                        lease_token=None,
                        lease_until=None,
                    )
                )
    if confirmed:
        logger.info("broker confirmed events=%d", len(confirmed))
    if len(confirmed) != len(settled):
        logger.warning(
            "broker did not confirm events=%d", len(settled) - len(confirmed)
        )


async def relay_lane(sessions: Sessions, settings: Settings, lane: Lane) -> None:
    failures = 0
    while True:
        try:
            async with await aio_pika.connect(
                settings.rabbitmq_url, timeout=5
            ) as connection:
                async with connection.channel(
                    publisher_confirms=True, on_return_raises=True
                ) as ch:
                    topology = await declare_topology(ch)
                    while not ch.is_closed:
                        token = uuid4()
                        events = await claim_events(sessions, lane, token)
                        failures = 0
                        if not events:
                            await asyncio.sleep(settings.outbox_poll_seconds)
                            continue
                        confirms = await asyncio.gather(
                            *(
                                publish_event(topology.exchange, lane, event)
                                for event in events
                            )
                        )
                        await settle_events(
                            sessions,
                            settings,
                            token,
                            list(zip(events, confirms, strict=True)),
                        )
        except Exception as exc:
            failures += 1
            logger.warning(
                "relay unavailable lane=%s error=%s", lane, type(exc).__name__
            )
            await asyncio.sleep(retry_delay(1, 30, failures))


async def run_relay(sessions: Sessions, settings: Settings) -> None:
    async with asyncio.TaskGroup() as group:
        group.create_task(relay_lane(sessions, settings, "bulk"))
        group.create_task(relay_lane(sessions, settings, "single"))
