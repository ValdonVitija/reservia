import logging
from typing import Annotated

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

MAX_BATCH_ROWS = 500
CHUNK_ROWS = 100
MAX_BODY_BYTES = 1_048_576
BACKLOG_SHARDS = 16
MAX_JOB_ATTEMPTS = 5

Positive = Annotated[int, Field(gt=0)]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RESERVATION_", extra="ignore")

    database_url: str = (
        "postgresql+psycopg://reservations:reservations@localhost:5432/reservations"
    )
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    db_pool_size: Positive = 8
    db_pool_timeout: Positive = 3
    bulk_backlog_rows: Annotated[int, Field(ge=MAX_BATCH_ROWS * BACKLOG_SHARDS)] = (
        100_000
    )
    single_backlog_rows: Annotated[int, Field(ge=BACKLOG_SHARDS)] = 4_000
    tenant_bulk_rows_per_minute: Positive = 100_000
    tenant_single_rows_per_minute: Positive = 6_000
    tenant_bulk_backlog_rows: Positive = 75_000
    tenant_single_backlog_rows: Positive = 2_000
    worker_concurrency: Positive = 4
    outbox_poll_seconds: Annotated[float, Field(gt=0)] = 0.25
    reconcile_after_seconds: Positive = 900
    retry_base_seconds: Annotated[float, Field(gt=0)] = 2
    retry_max_seconds: Annotated[float, Field(gt=0)] = 60

    @model_validator(mode="after")
    def check_retry_delay(self) -> "Settings":
        if self.retry_max_seconds < self.retry_base_seconds:
            raise ValueError("maximum retry delay must be at least the base delay")
        return self


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
