from collections.abc import Iterator
from itertools import batched
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from reservation_ingest.db import Sessions
from reservation_ingest.models import Housing, Integration, Room, Tenant

DEFAULT_INTEGRATION_NAME = "pms"


def seeded_tenant(number: int) -> UUID:
    return uuid5(NAMESPACE_URL, f"reservation-ingest/tenant/{number}")


def seeded_housing(tenant: UUID, number: int) -> UUID:
    return uuid5(tenant, f"housing/{number}")


def seeded_room(housing: UUID, number: int) -> UUID:
    return uuid5(housing, f"room/{number}")


def seeded_housings(owners: int, per_owner: int) -> Iterator[tuple[int, int, UUID]]:
    for owner in range(1, owners + 1):
        tenant_id = seeded_tenant(owner)
        for number in range(1, per_owner + 1):
            yield owner, number, seeded_housing(tenant_id, number)


async def seed(
    sessions: Sessions, owners: int, per_owner: int, rooms: int
) -> dict[str, object]:
    for chunk in batched(range(1, owners + 1), 500, strict=False):
        async with sessions.begin() as session:
            await session.execute(
                insert(Tenant).on_conflict_do_nothing(),
                [{"id": seeded_tenant(i), "name": f"Seed tenant {i}"} for i in chunk],
            )
            await session.execute(
                insert(Integration).on_conflict_do_nothing(
                    index_elements=[Integration.tenant_id, Integration.name]
                ),
                [
                    {
                        "id": uuid5(
                            seeded_tenant(i), f"integration/{DEFAULT_INTEGRATION_NAME}"
                        ),
                        "tenant_id": seeded_tenant(i),
                        "name": DEFAULT_INTEGRATION_NAME,
                    }
                    for i in chunk
                ],
            )
    for properties in batched(seeded_housings(owners, per_owner), 500, strict=False):
        async with sessions.begin() as session:
            await session.execute(
                insert(Housing).on_conflict_do_nothing(),
                [
                    {
                        "id": housing,
                        "tenant_id": seeded_tenant(owner),
                        "name": f"Seed property {owner}-{number}",
                        "kind": "HOT" if rooms else "HOU",
                    }
                    for owner, number, housing in properties
                ],
            )
            room_rows = (
                {
                    "id": seeded_room(housing, number),
                    "housing_id": housing,
                    "number": str(number),
                }
                for _, _, housing in properties
                for number in range(1, rooms + 1)
            )
            for chunk in batched(room_rows, 500, strict=False):
                await session.execute(
                    insert(Room).on_conflict_do_nothing(), list(chunk)
                )
    first_tenant = seeded_tenant(1)
    first_housing = seeded_housing(first_tenant, 1)
    async with sessions() as session:
        integration = (
            await session.scalars(
                select(Integration.id).where(
                    Integration.tenant_id == first_tenant,
                    Integration.name == DEFAULT_INTEGRATION_NAME,
                )
            )
        ).one()
    return {
        "owners": owners,
        "properties": owners * per_owner,
        "rooms": owners * per_owner * rooms,
        "tenant_id": str(first_tenant),
        "integration_id": str(integration),
        "housing": str(first_housing),
        "room_id": str(seeded_room(first_housing, 1)) if rooms else None,
    }


async def add_tenant(sessions: Sessions, name: str) -> UUID:
    tenant_id = uuid4()
    async with sessions.begin() as session:
        session.add(Tenant(id=tenant_id, name=name))
    return tenant_id


async def add_housing(
    sessions: Sessions, tenant_id: UUID, name: str, room_numbers: list[str]
) -> UUID:
    housing_id = uuid4()
    async with sessions.begin() as session:
        if await session.get(Tenant, tenant_id) is None:
            raise ValueError("unknown tenant")
        await session.execute(
            insert(Housing).values(
                id=housing_id,
                tenant_id=tenant_id,
                name=name,
                kind="HOT" if room_numbers else "HOU",
            )
        )
        if room_numbers:
            await session.execute(
                insert(Room),
                [
                    {"id": uuid4(), "housing_id": housing_id, "number": n}
                    for n in room_numbers
                ],
            )
    return housing_id


async def add_integration(sessions: Sessions, tenant_id: UUID, name: str) -> UUID:
    async with sessions.begin() as session:
        if await session.get(Tenant, tenant_id) is None:
            raise ValueError("unknown tenant")
        await session.execute(
            insert(Integration)
            .values(id=uuid4(), tenant_id=tenant_id, name=name)
            .on_conflict_do_nothing(
                index_elements=[Integration.tenant_id, Integration.name]
            )
        )
        return (
            await session.scalars(
                select(Integration.id).where(
                    Integration.tenant_id == tenant_id, Integration.name == name
                )
            )
        ).one()
