from dataclasses import dataclass

from aio_pika import ExchangeType, abc
from pamqp.common import FieldTable

from reservation_ingest.schemas import Lane


@dataclass(frozen=True, slots=True)
class Topology:
    exchange: abc.AbstractExchange
    bulk: abc.AbstractQueue
    single: abc.AbstractQueue
    dead: abc.AbstractQueue

    def queue_for(self, lane: Lane) -> abc.AbstractQueue:
        return self.bulk if lane == "bulk" else self.single


async def declare_topology(channel: abc.AbstractChannel) -> Topology:
    work = await channel.declare_exchange(
        "reservations.work", ExchangeType.DIRECT, durable=True
    )
    dead_exchange = await channel.declare_exchange(
        "reservations.dead", ExchangeType.DIRECT, durable=True
    )
    dead = await channel.declare_queue(
        "reservations.dead", durable=True, arguments={"x-queue-type": "quorum"}
    )
    await dead.bind(dead_exchange, routing_key="dead")
    args: FieldTable = {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "reservations.dead",
        "x-dead-letter-routing-key": "dead",
        "x-delivery-limit": 10,
        "x-overflow": "reject-publish",
    }
    bulk = await channel.declare_queue(
        "reservations.bulk", durable=True, arguments=args
    )
    single = await channel.declare_queue(
        "reservations.single", durable=True, arguments=args
    )
    await bulk.bind(work, routing_key="bulk")
    await single.bind(work, routing_key="single")
    return Topology(work, bulk, single, dead)
