import hashlib
import json
import re
from datetime import date
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    WithJsonSchema,
    field_validator,
    model_validator,
)

from reservation_ingest.config import MAX_BATCH_ROWS

Lane = Literal["bulk", "single"]
ItemState = Literal["pending", "completed", "rejected", "failed"]
IntakeState = Literal[
    "accepted", "processing", "completed", "completed_with_errors", "failed"
]
ExternalText = Annotated[str, Field(max_length=255)]


class GuestGroup(BaseModel):
    model_config = ConfigDict(extra="forbid")

    known_number_of_guests: Annotated[int, Field(strict=True, gt=0, le=2_147_483_647)]


class ReservationInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    housing: UUID
    check_in_date: date
    check_out_date: date
    guest_group: GuestGroup
    room_ids: list[UUID] = Field(default_factory=list, max_length=500)
    external_id: ExternalText | None = None
    booking_reference: ExternalText | None = None
    source_name: ExternalText | None = None
    accommodation_type: (
        Literal[
            "",
            "DOUBLE_ROOM_DOUBLE_USE",
            "DOUBLE_ROOM_SINGLE_USE",
            "STUDIO",
            "APARTMENT_2_4_PAX",
            "APARTMENT_4_6_PAX",
            "OTHER",
        ]
        | None
    ) = None
    default_leader_full_name: ExternalText | None = None
    default_invite_email: ExternalText | None = None
    default_phone_number: Annotated[str, Field(max_length=40)] | None = None
    default_email_language: Annotated[str, Field(max_length=16)] | None = None

    @field_validator("check_in_date", "check_out_date", mode="before")
    @classmethod
    def require_iso_date(cls, value: object) -> date:
        if (
            not isinstance(value, str)
            or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value) is None
        ):
            raise ValueError("use a calendar date in YYYY-MM-DD format")
        return date.fromisoformat(value)

    @field_validator(
        "external_id",
        "booking_reference",
        "source_name",
        "default_leader_full_name",
        "default_invite_email",
        "default_phone_number",
        "default_email_language",
    )
    @classmethod
    def require_database_text(cls, value: str | None) -> str | None:
        if value is not None:
            if chr(0) in value:
                raise ValueError("text cannot contain a null character")
            try:
                value.encode()
            except UnicodeEncodeError as exc:
                raise ValueError("text must contain valid Unicode characters") from exc
        return value

    @field_validator("external_id")
    @classmethod
    def normalize_external_id(cls, value: str | None) -> str | None:
        return (value.strip() or None) if value is not None else None

    @field_validator("room_ids")
    @classmethod
    def normalize_room_ids(cls, value: list[UUID]) -> list[UUID]:
        return sorted(set(value))

    @model_validator(mode="after")
    def check_stay(self) -> "ReservationInput":
        if self.check_out_date <= self.check_in_date:
            raise ValueError("check-out must follow check-in")
        return self


BulkRow = Annotated[
    JsonValue, WithJsonSchema({"$ref": "#/components/schemas/ReservationInput"})
]


class BatchInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Individual rows are validated independently by the processing worker.
    reservations: Annotated[
        list[BulkRow], Field(min_length=1, max_length=MAX_BATCH_ROWS)
    ]


class AcceptedReceipt(BaseModel):
    id: UUID
    status_url: str


class ResultItem(BaseModel):
    row_index: int
    state: ItemState
    reservation_id: UUID | None
    error_code: str | None


class ReceiptStatus(BaseModel):
    id: UUID
    state: IntakeState
    total: int
    pending: int
    succeeded: int
    rejected: int
    failed: int
    items: list[ResultItem]
    next_after: int | None


class DispatchMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = 1
    event_id: UUID
    job_id: UUID
    generation: Annotated[int, Field(strict=True, gt=0)]


def canonical_json(value: object) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )


def canonical_hash(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()
