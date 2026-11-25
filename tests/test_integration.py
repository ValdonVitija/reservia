import asyncio
import os
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import func, select

from reservation_ingest.api import create_app
from reservation_ingest.catalog import (
    DEFAULT_INTEGRATION_NAME,
    add_integration,
    seed,
    seeded_housing,
    seeded_room,
    seeded_tenant,
)
from reservation_ingest.config import Settings
from reservation_ingest.db import create_database
from reservation_ingest.models import Housing

pytestmark = pytest.mark.integration


@dataclass(frozen=True)
class System:
    api: str
    other_integration_id: str


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def system() -> System:
    api = os.environ.get("RESERVATION_TEST_URL")
    if not api:
        pytest.skip("set RESERVATION_TEST_URL for the running integration stack")
    engine, sessions = create_database(Settings())
    try:
        await seed(sessions, 2, 2, 2)
        other = await add_integration(
            sessions, seeded_tenant(2), DEFAULT_INTEGRATION_NAME
        )
    finally:
        await engine.dispose()
    return System(api, str(other))


def booking(external_id: str | None = None) -> dict[str, Any]:
    housing = seeded_housing(seeded_tenant(1), 1)
    return {
        "housing": str(housing),
        "external_id": external_id,
        "check_in_date": "2027-04-12",
        "check_out_date": "2027-04-15",
        "guest_group": {"known_number_of_guests": 2},
        "room_ids": [str(seeded_room(housing, 1))],
    }


async def terminal(client: httpx.AsyncClient, path: str) -> dict[str, Any]:
    async with asyncio.timeout(25):
        while True:
            response = await client.get(path, params={"limit": 500})
            response.raise_for_status()
            data = response.json()
            if not data["pending"]:
                return data
            await asyncio.sleep(0.2)


async def test_batch_partial_results_and_pagination(system: System) -> None:
    bad_room = str(seeded_room(seeded_housing(seeded_tenant(1), 2), 1))
    prefix = uuid4().hex
    rows = [booking(f"{prefix}-{index}") for index in range(205)]
    rows[19]["check_out_date"] = "2027-04-10"
    rows[100]["housing"] = str(seeded_housing(seeded_tenant(2), 1))
    rows[201]["room_ids"] = [bad_room]
    rows[204]["source_name"] = chr(0)
    async with httpx.AsyncClient(base_url=system.api) as client:
        request = {"reservations": rows}
        key = uuid4().hex
        first = await client.post(
            "/v1/reservation-imports", json=request, headers={"Idempotency-Key": key}
        )
        assert first.status_code == 202, first.text
        path = first.headers["Location"]
        page = (await client.get(path, params={"limit": 2})).json()
        assert [item["row_index"] for item in page["items"]] == [0, 1]
        assert page["next_after"] == 1
        other_scope = await client.get(
            path, headers={"X-Integration-Id": system.other_integration_id}
        )
        assert other_scope.status_code == 404
        data = await terminal(client, path)
        assert (data["succeeded"], data["rejected"], data["failed"]) == (201, 4, 0)
        assert data["state"] == "completed_with_errors"
        assert data["items"][19]["error_code"] == "INVALID_RESERVATION"
        assert data["items"][100]["error_code"] == "UNKNOWN_HOUSING"
        assert data["items"][201]["error_code"] == "ROOM_NOT_IN_HOUSING"
        assert data["items"][204]["error_code"] == "INVALID_RESERVATION"
        assert {item["row_index"] for item in data["items"]} == set(range(205))
        retry = await client.post(
            "/v1/reservation-imports", json=request, headers={"Idempotency-Key": key}
        )
        assert retry.status_code == 200
        assert retry.headers["Location"] == path
        conflict = await client.post(
            "/v1/reservation-imports",
            json={"reservations": rows[:1]},
            headers={"Idempotency-Key": key},
        )
        assert conflict.status_code == 409


async def test_concurrent_single_and_bulk_share_reservation_identity(
    system: System,
) -> None:
    async with httpx.AsyncClient(base_url=system.api) as client:
        booking_row = booking(uuid4().hex)
        single, batch = await asyncio.gather(
            client.post(
                "/v1/reservations",
                json=booking_row,
                headers={"Idempotency-Key": uuid4().hex},
            ),
            client.post(
                "/v1/reservation-imports",
                json={"reservations": [booking_row]},
                headers={"Idempotency-Key": uuid4().hex},
            ),
        )
        assert single.status_code == batch.status_code == 202
        results = await asyncio.gather(
            terminal(client, single.headers["Location"]),
            terminal(client, batch.headers["Location"]),
        )
        assert all(record["succeeded"] == 1 for record in results)
        assert (
            results[0]["items"][0]["reservation_id"]
            == results[1]["items"][0]["reservation_id"]
        )
        booking_row["check_out_date"] = "2027-05-10"
        conflict = await client.post(
            "/v1/reservations",
            json=booking_row,
            headers={"Idempotency-Key": uuid4().hex},
        )
        assert conflict.status_code == 202
        outcome = await terminal(client, conflict.headers["Location"])
        assert outcome["items"][0]["error_code"] == "EXTERNAL_ID_CONFLICT"


async def test_seed_scales_and_preserves_ids(system: System) -> None:
    engine, sessions = create_database(Settings())
    try:
        first = await seed(sessions, 100, 100, 0)
        second = await seed(sessions, 100, 100, 0)
        assert first["housing"] == second["housing"]
        assert first["integration_id"] == second["integration_id"]
        async with sessions() as session:
            count = await session.scalar(
                select(func.count(Housing.id)).where(
                    Housing.tenant_id.in_([seeded_tenant(i) for i in range(1, 101)])
                )
            )
            assert count == 10_000
    finally:
        await engine.dispose()


async def test_tenant_backlog_stops_only_that_tenant(system: System) -> None:
    app = create_app(Settings(tenant_bulk_backlog_rows=2))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client,
    ):
        response = await client.post(
            "/v1/reservation-imports",
            json={"reservations": [booking(uuid4().hex) for _ in range(3)]},
            headers={"Idempotency-Key": uuid4().hex},
        )
        assert response.status_code == 429, response.text
        assert response.json()["detail"]["code"] == "TENANT_BACKLOG_FULL"
        other_booking = booking(uuid4().hex) | {
            "housing": str(seeded_housing(seeded_tenant(2), 1)),
            "room_ids": [],
        }
        other = await client.post(
            "/v1/reservation-imports",
            json={"reservations": [other_booking]},
            headers={
                "Idempotency-Key": uuid4().hex,
                "X-Integration-Id": system.other_integration_id,
            },
        )
        assert other.status_code == 202, other.text
        client.headers["X-Integration-Id"] = system.other_integration_id
        outcome = await terminal(client, other.headers["Location"])
        assert outcome["succeeded"] == 1
