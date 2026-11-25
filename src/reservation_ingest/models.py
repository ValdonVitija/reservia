from datetime import date, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(String(200))


class Integration(Base):
    __tablename__ = "integrations"
    __table_args__ = (UniqueConstraint("tenant_id", "name"),)

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"))
    name: Mapped[str] = mapped_column(String(200))


class Credential(Base):
    __tablename__ = "credentials"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    integration_id: Mapped[UUID] = mapped_column(ForeignKey("integrations.id"))
    digest: Mapped[str] = mapped_column(String(64))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Housing(Base):
    __tablename__ = "housings"
    __table_args__ = (Index("ix_housings_tenant", "tenant_id"),)

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"))
    name: Mapped[str] = mapped_column(String(200))
    kind: Mapped[str] = mapped_column(String(3))


class Room(Base):
    __tablename__ = "rooms"
    __table_args__ = (UniqueConstraint("housing_id", "number"),)

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    housing_id: Mapped[UUID] = mapped_column(ForeignKey("housings.id"))
    number: Mapped[str] = mapped_column(String(40))


class ImportReceipt(Base):
    __tablename__ = "import_receipts"
    __table_args__ = (UniqueConstraint("integration_id", "kind", "idempotency_key"),)

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"))
    integration_id: Mapped[UUID] = mapped_column(ForeignKey("integrations.id"))
    kind: Mapped[str] = mapped_column(String(10))
    idempotency_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    payload: Mapped[str | None] = mapped_column(Text)
    row_count: Mapped[int] = mapped_column(Integer)
    budget_shard: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        UniqueConstraint("import_id", "start_index"),
        Index(
            "ix_jobs_reconciliation",
            "updated_at",
            "id",
            postgresql_where=text("state = 'pending'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    import_id: Mapped[UUID] = mapped_column(ForeignKey("import_receipts.id"))
    start_index: Mapped[int] = mapped_column(Integer)
    row_count: Mapped[int] = mapped_column(Integer)
    state: Mapped[str] = mapped_column(String(12), server_default="pending")
    generation: Mapped[int] = mapped_column(Integer, server_default="1")
    attempts: Mapped[int] = mapped_column(Integer, server_default="0")
    succeeded: Mapped[int] = mapped_column(Integer, server_default="0")
    rejected: Mapped[int] = mapped_column(Integer, server_default="0")
    failed: Mapped[int] = mapped_column(Integer, server_default="0")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        UniqueConstraint("job_id", "generation"),
        Index(
            "ix_outbox_ready",
            "available_at",
            "id",
            postgresql_where=text("sent_at IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    job_id: Mapped[UUID] = mapped_column(ForeignKey("jobs.id"))
    generation: Mapped[int] = mapped_column(Integer)
    lane: Mapped[str] = mapped_column(String(10))
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    lease_token: Mapped[UUID | None] = mapped_column(Uuid)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_failures: Mapped[int] = mapped_column(Integer, server_default="0")


class Reservation(Base):
    __tablename__ = "reservations"
    __table_args__ = (
        Index(
            "uq_reservations_external",
            "integration_id",
            "housing_id",
            "external_id",
            unique=True,
            postgresql_where=text("external_id IS NOT NULL"),
        ),
        CheckConstraint("check_out_date > check_in_date"),
        CheckConstraint("guest_count > 0"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    integration_id: Mapped[UUID] = mapped_column(ForeignKey("integrations.id"))
    housing_id: Mapped[UUID] = mapped_column(ForeignKey("housings.id"))
    external_id: Mapped[str | None] = mapped_column(String(255))
    check_in_date: Mapped[date] = mapped_column(Date)
    check_out_date: Mapped[date] = mapped_column(Date)
    guest_count: Mapped[int] = mapped_column(Integer)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    payload_hash: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class ItemResult(Base):
    __tablename__ = "item_results"

    import_id: Mapped[UUID] = mapped_column(
        ForeignKey("import_receipts.id"), primary_key=True
    )
    row_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    state: Mapped[str] = mapped_column(String(12))
    reservation_id: Mapped[UUID | None] = mapped_column(ForeignKey("reservations.id"))
    error_code: Mapped[str | None] = mapped_column(String(48))


class TenantUsage(Base):
    __tablename__ = "tenant_usage"

    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"), primary_key=True)
    lane: Mapped[str] = mapped_column(String(10), primary_key=True)
    minute: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    rows: Mapped[int] = mapped_column(Integer)


class BacklogBudget(Base):
    __tablename__ = "backlog_budgets"
    __table_args__ = (CheckConstraint("pending_rows >= 0"),)

    lane: Mapped[str] = mapped_column(String(10), primary_key=True)
    shard: Mapped[int] = mapped_column(Integer, primary_key=True)
    pending_rows: Mapped[int] = mapped_column(Integer)


class TenantBacklog(Base):
    __tablename__ = "tenant_backlogs"
    __table_args__ = (CheckConstraint("pending_rows >= 0"),)

    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"), primary_key=True)
    lane: Mapped[str] = mapped_column(String(10), primary_key=True)
    pending_rows: Mapped[int] = mapped_column(Integer)


class Quarantine(Base):
    __tablename__ = "quarantine"

    digest: Mapped[str] = mapped_column(String(64), primary_key=True)
    job_id: Mapped[UUID | None] = mapped_column(Uuid)
    error_code: Mapped[str] = mapped_column(String(48))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
