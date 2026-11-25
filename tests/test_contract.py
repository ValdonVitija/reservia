from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from reservation_ingest.api import create_app
from reservation_ingest.config import MAX_BODY_BYTES
from reservation_ingest.schemas import ReservationInput


def booking() -> dict[str, object]:
    return {
        "housing": str(uuid4()),
        "check_in_date": "2027-04-12",
        "check_out_date": "2027-04-15",
        "guest_group": {"known_number_of_guests": 2},
    }


@pytest.mark.parametrize(
    "fields",
    [
        {"check_out_date": "2027-04-10"},
        {"check_in_date": "2027-W12-1"},
        {"guest_group": {"known_number_of_guests": 0}},
        {"source_name": chr(0)},
    ],
)
def test_invalid_booking(fields: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ReservationInput.model_validate(booking() | fields)


async def test_http_contract_without_database() -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app()), base_url="http://test"
    ) as client:
        assert (await client.get("/health/live")).status_code == 200
        oversized = await client.post(
            "/v1/reservation-imports",
            headers={"Content-Type": "application/json"},
            content=b" " * (MAX_BODY_BYTES + 1),
        )
        assert oversized.status_code == 413
        document = (await client.get("/openapi.json")).json()
        assert "securitySchemes" not in document["components"]
        for operations in document["paths"].values():
            assert all("security" not in operation for operation in operations.values())
