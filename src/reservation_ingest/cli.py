import asyncio
import json
import signal
from collections.abc import Awaitable, Callable, Coroutine
from typing import Annotated, Any
from uuid import UUID

import typer

from reservation_ingest.catalog import (
    add_housing,
    add_integration,
    add_tenant,
    seed,
)
from reservation_ingest.config import Settings, configure_logging
from reservation_ingest.db import Sessions, create_database
from reservation_ingest.intake import IntakeError
from reservation_ingest.operations import prune, stats
from reservation_ingest.processing import replay_job
from reservation_ingest.relay import run_relay
from reservation_ingest.schemas import Lane
from reservation_ingest.worker import run_worker

app = typer.Typer(
    add_completion=False,
    context_settings={"help_option_names": ["-h", "--help"]},
    pretty_exceptions_enable=False,
)


@app.callback(invoke_without_command=True)
def show_help(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help(), color=ctx.color)


async def service(coro: Coroutine[Any, Any, None]) -> None:
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(coro)
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, task.cancel)
    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(signum)


def run_with_database[T](
    command: Callable[[Sessions, Settings], Awaitable[T]],
) -> T:
    async def run() -> T:
        settings = Settings()
        engine, sessions = create_database(settings)
        try:
            return await command(sessions, settings)
        finally:
            await engine.dispose()

    return asyncio.run(run())


@app.command()
def relay() -> None:
    run_with_database(lambda sessions, settings: service(run_relay(sessions, settings)))


@app.command()
def worker(lane: Annotated[Lane, typer.Option()]) -> None:
    async def run(sessions: Sessions, settings: Settings) -> None:
        required_connections = settings.worker_concurrency + int(lane == "bulk")
        if required_connections > settings.db_pool_size:
            raise ValueError("worker concurrency exceeds its database pool size")
        await service(run_worker(sessions, settings, lane))

    run_with_database(run)


@app.command("seed")
def seed_catalog(
    owners: Annotated[int, typer.Option(min=1)] = 1,
    properties_per_owner: Annotated[int, typer.Option(min=1)] = 1,
    rooms_per_property: Annotated[int, typer.Option(min=0)] = 0,
) -> None:
    catalog = run_with_database(
        lambda sessions, _: seed(
            sessions, owners, properties_per_owner, rooms_per_property
        )
    )
    typer.echo(json.dumps(catalog))


@app.command("add-tenant")
def create_tenant(name: Annotated[str, typer.Option()]) -> None:
    tenant_id = run_with_database(lambda sessions, _: add_tenant(sessions, name))
    typer.echo(json.dumps({"tenant_id": str(tenant_id)}))


@app.command("add-housing")
def create_housing(
    tenant_id: Annotated[UUID, typer.Option()],
    name: Annotated[str, typer.Option()],
    rooms: Annotated[list[str] | None, typer.Option("--room")] = None,
) -> None:
    room_numbers = list(dict.fromkeys(rooms or []))
    housing_id = run_with_database(
        lambda sessions, _: add_housing(sessions, tenant_id, name, room_numbers)
    )
    typer.echo(json.dumps({"housing": str(housing_id)}))


@app.command("add-integration")
def create_integration(
    tenant_id: Annotated[UUID, typer.Option()],
    name: Annotated[str, typer.Option()],
) -> None:
    integration_id = run_with_database(
        lambda sessions, _: add_integration(sessions, tenant_id, name)
    )
    typer.echo(json.dumps({"integration_id": str(integration_id)}))


@app.command()
def replay(job_id: Annotated[UUID, typer.Argument()]) -> None:
    receipt_id = run_with_database(
        lambda sessions, settings: replay_job(sessions, settings, job_id)
    )
    typer.echo(json.dumps({"id": str(receipt_id), "job_id": str(job_id)}))


@app.command("stats")
def show_stats() -> None:
    typer.echo(json.dumps(run_with_database(stats)))


@app.command("prune")
def prune_payloads(retention_days: Annotated[int, typer.Option(min=1)] = 7) -> None:
    count = run_with_database(lambda sessions, _: prune(sessions, retention_days))
    typer.echo(json.dumps({"payloads_pruned": count}))


def main() -> None:
    configure_logging()
    try:
        app(prog_name="reservation-ingest")
    except (ValueError, IntakeError) as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
