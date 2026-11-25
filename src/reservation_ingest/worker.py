import asyncio
import logging
from typing import Literal

import aio_pika
from aio_pika import abc
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from reservation_ingest.broker import declare_topology
from reservation_ingest.config import Settings
from reservation_ingest.db import Sessions
from reservation_ingest.processing import (
    process_job,
    quarantine_message,
    retry_delay,
    retry_job,
)
from reservation_ingest.schemas import DispatchMessage, Lane

logger = logging.getLogger(__name__)

_RETRY_SQL_STATES = {"40001", "40P01", "55P03", "57014"}


async def handle_work(
    message: abc.AbstractIncomingMessage, sessions: Sessions, settings: Settings
) -> None:
    try:
        dispatch = DispatchMessage.model_validate_json(message.body)
    except ValidationError:
        await message.reject(requeue=False)
        return
    try:
        await process_job(sessions, dispatch)
    except DBAPIError as exc:
        if (
            exc.connection_invalidated
            or getattr(exc.orig, "sqlstate", None) not in _RETRY_SQL_STATES
        ):
            raise
        await retry_job(sessions, settings, dispatch)
    except Exception as exc:
        logger.warning(
            "job processing error job=%s error=%s", dispatch.job_id, type(exc).__name__
        )
        await retry_job(sessions, settings, dispatch)
    await message.ack()


async def consume(
    sessions: Sessions,
    settings: Settings,
    lane: Lane | Literal["dead"],
    concurrency: int,
) -> None:
    failures = 0
    while True:
        try:
            # Avoid repeatedly consuming and requeuing during a database outage.
            async with sessions() as session:
                await session.execute(text("SELECT 1"))
            async with await aio_pika.connect(
                settings.rabbitmq_url, timeout=5
            ) as connection:
                async with connection.channel() as channel:
                    await channel.set_qos(prefetch_count=concurrency)
                    topology = await declare_topology(channel)
                    queue = (
                        topology.dead if lane == "dead" else topology.queue_for(lane)
                    )
                    slots = asyncio.Semaphore(concurrency)

                    async def handle(
                        message: abc.AbstractIncomingMessage,
                        slots: asyncio.Semaphore,
                    ) -> None:
                        nonlocal failures
                        try:
                            if lane == "dead":
                                await quarantine_message(sessions, message.body)
                                await message.ack()
                            else:
                                await handle_work(message, sessions, settings)
                            failures = 0
                        finally:
                            slots.release()

                    async with (
                        queue.iterator() as messages,
                        asyncio.TaskGroup() as tasks,
                    ):
                        async for message in messages:
                            await slots.acquire()
                            tasks.create_task(handle(message, slots))
        except Exception as exc:
            failures += 1
            logger.warning(
                "consumer disconnected lane=%s error=%s", lane, type(exc).__name__
            )
            await asyncio.sleep(retry_delay(1, 30, failures))


async def run_worker(sessions: Sessions, settings: Settings, lane: Lane) -> None:
    async with asyncio.TaskGroup() as group:
        group.create_task(
            consume(sessions, settings, lane, settings.worker_concurrency)
        )
        if lane == "bulk":
            group.create_task(consume(sessions, settings, "dead", 1))
