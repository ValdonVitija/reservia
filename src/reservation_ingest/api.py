import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, TimeoutError
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from reservation_ingest.config import MAX_BODY_BYTES, Settings, configure_logging
from reservation_ingest.db import create_database
from reservation_ingest.intake import (
    IntakeError,
    Partner,
    receipt_status,
    resolve_integration,
    submit,
)
from reservation_ingest.schemas import (
    AcceptedReceipt,
    BatchInput,
    ReceiptStatus,
    ReservationInput,
)

logger = logging.getLogger(__name__)
IdempotencyKey = Annotated[
    str,
    Header(alias="Idempotency-Key", min_length=1, max_length=128, pattern=r"^[!-~]+$"),
]
After = Annotated[int, Query(ge=-1)]
Limit = Annotated[int, Query(ge=1, le=500)]
IntegrationId = Annotated[
    UUID | None,
    Header(
        alias="X-Integration-Id",
        description="Select an integration. Defaults to the first seeded integration.",
    ),
]


class LimitRequestBody:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        received = 0

        async def bounded_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > MAX_BODY_BYTES:
                    raise HTTPException(413, detail={"code": "BODY_TOO_LARGE"})
            return message

        await self.app(scope, bounded_receive, send)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = Settings() if settings is None else settings
    engine, sessions = create_database(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging()
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(
        title="Reservation Ingest API",
        description="Asynchronous reservation intake for hospitality integrations.",
        lifespan=lifespan,
    )
    app.add_middleware(LimitRequestBody)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            {
                "detail": {
                    "code": "INVALID_REQUEST",
                    "fields": [
                        {"location": issue["loc"], "type": issue["type"]}
                        for issue in exc.errors()
                    ],
                },
            },
            status_code=422,
        )

    @app.exception_handler(IntakeError)
    async def intake_error(request: Request, exc: IntakeError) -> JSONResponse:
        headers = {}
        if exc.retry_after is not None:
            headers["Retry-After"] = str(exc.retry_after)
        return JSONResponse(
            {"detail": {"code": exc.code}}, status_code=exc.status, headers=headers
        )

    @app.exception_handler(DBAPIError)
    @app.exception_handler(TimeoutError)
    async def database_error(request: Request, exc: Exception) -> JSONResponse:
        logger.warning(
            "database operation failed path=%s error=%s", request.url.path, type(exc)
        )
        return JSONResponse(
            {"detail": {"code": "DATABASE_UNAVAILABLE"}},
            status_code=503,
            headers={"Retry-After": "5"},
        )

    async def partner_for(integration_id: IntegrationId = None) -> Partner:
        return await resolve_integration(sessions, integration_id)

    @app.get("/health/live", include_in_schema=False)
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", include_in_schema=False)
    async def ready() -> dict[str, str]:
        async with sessions() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ok"}

    @app.post(
        "/v1/reservations",
        status_code=202,
        responses={200: {"model": AcceptedReceipt}},
        tags=["Reservations"],
    )
    async def create_reservation(
        body: ReservationInput,
        response: Response,
        key: IdempotencyKey,
        partner: Annotated[Partner, Depends(partner_for)],
    ) -> AcceptedReceipt:
        receipt_id, created = await submit(
            sessions, settings, partner, "single", key, [body.model_dump(mode="json")]
        )
        status_url = f"/v1/reservation-intakes/{receipt_id}"
        response.status_code = 202 if created else 200
        response.headers["Location"] = status_url
        return AcceptedReceipt(id=receipt_id, status_url=status_url)

    @app.post(
        "/v1/reservation-imports",
        status_code=202,
        responses={200: {"model": AcceptedReceipt}},
        tags=["Imports"],
    )
    async def create_import(
        body: BatchInput,
        response: Response,
        key: IdempotencyKey,
        partner: Annotated[Partner, Depends(partner_for)],
    ) -> AcceptedReceipt:
        receipt_id, created = await submit(
            sessions, settings, partner, "bulk", key, body.reservations
        )
        status_url = f"/v1/reservation-imports/{receipt_id}"
        response.status_code = 202 if created else 200
        response.headers["Location"] = status_url
        return AcceptedReceipt(id=receipt_id, status_url=status_url)

    @app.get("/v1/reservation-intakes/{receipt_id}", tags=["Reservations"])
    async def get_reservation(
        receipt_id: UUID,
        response: Response,
        partner: Annotated[Partner, Depends(partner_for)],
    ) -> ReceiptStatus:
        response.headers["Cache-Control"] = "no-store"
        return await receipt_status(sessions, partner, "single", receipt_id, -1, 1)

    @app.get("/v1/reservation-imports/{receipt_id}", tags=["Imports"])
    async def get_import(
        receipt_id: UUID,
        response: Response,
        partner: Annotated[Partner, Depends(partner_for)],
        after: After = -1,
        limit: Limit = 100,
    ) -> ReceiptStatus:
        response.headers["Cache-Control"] = "no-store"
        return await receipt_status(sessions, partner, "bulk", receipt_id, after, limit)

    return app


app = create_app()
