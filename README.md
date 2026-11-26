# Reservia

An API for external services to send confirmed reservations individually or in bulk. The  services use PostgreSQL for durable intake and RabbitMQ to dispatch work. The architecture has been designed/constructed to support reservation processing even in the event of failure

[Production operations](ops/PRODUCTION.md)

## Explore the architecture

From a local checkout, double-click [architecture.html](docs/architecture-reservation-platform-20261125-220703/architecture.html) to open the viewer in your browser.



![Reservation ingestion architecture](docs/architecture-reservation-platform-20261125-220703/architecture.svg)

## Run locally

Install a Compose-compatible container runtime. From the repository root, start the services and seed a sample property.

```bash
docker compose up --build -d
docker compose exec api reservation-ingest seed --rooms-per-property 2
```

Use the seeded `housing` UUID in reservation requests and supply an `Idempotency-Key` for each new submission. Try both endpoints directly in Swagger. The API returns a status URL for tracking each submission. [OpenAPI docs](http://localhost:8000/docs)

Stop the stack with `docker compose down`. Local data lives in named volumes. If your system provides Compose as `docker-compose`, use that command in place of `docker compose`.

## CLI

Run `reservation-ingest` from the repository root to list commands. Add `--help` after a command to see its options.

```bash
uv run reservation-ingest
uv run reservation-ingest seed --help
```

Run the examples below against the running Compose stack. Using `exec` gives the CLI the container's database and broker configuration.

### Seed a test catalog

This example seeds two owners with three properties each and four rooms per property. Repeating it reuses the same identifiers.

```bash
docker compose exec -T api reservation-ingest seed \
  --owners 2 --properties-per-owner 3 --rooms-per-property 4
```

Copy `housing` from the JSON output into a reservation request in Swagger. It identifies the first seeded property, and requests default to the first seeded integration. To test bulk imports for one property, use that same `housing` UUID on every row. [Swagger](http://localhost:8000/docs)

### Create a property and integration

Create an owner account, add a hotel with rooms `101` and `102`, and register an integration for that owner.

```bash
tenant_id=$(docker compose exec -T api reservation-ingest add-tenant --name "Harbor Hotels" \
  | uv run python -c 'import json, sys; print(json.load(sys.stdin)["tenant_id"])')

docker compose exec -T api reservation-ingest add-housing \
  --tenant-id "$tenant_id" --name "Harbor Hotel" --room 101 --room 102

docker compose exec -T api reservation-ingest add-integration \
  --tenant-id "$tenant_id" --name pms
```

Use the returned `housing` UUID in the reservation body and the returned `integration_id` as the `X-Integration-Id` header on submissions and status requests for that owner.

### Inspect and recover imports

Run `stats` to see pending rows, queue counts, and failed or overdue job IDs.

```bash
docker compose exec -T api reservation-ingest stats
```

After resolving a failure, replace `JOB_UUID` with an ID from `failed_job_ids` or `overdue_job_ids`. Replay requires the staged import payload to be available.

```bash
docker compose exec -T api reservation-ingest replay JOB_UUID
```

Use `prune` to clear eligible staged payloads past the retention window. This example sets the window to 14 days.

```bash
docker compose exec -T api reservation-ingest prune --retention-days 14
```

Compose starts the `relay`, `worker --lane bulk`, and `worker --lane single` commands as background services. [Production operations](ops/PRODUCTION.md)

## Development

```bash
uv sync
uv run ruff check .
uv run pyright
uv run pytest
```

The Compose stack exposes PostgreSQL on port 15432 and RabbitMQ on port 15673, both on localhost. Set `RESERVATION_TEST_URL=http://localhost:8000` and `RESERVATION_DATABASE_URL=postgresql+psycopg://reservations:reservations@localhost:15432/reservations` to include the integration suite against a running stack.
