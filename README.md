# Reservia

An API for hospitality partners to send confirmed reservations individually or in bulk. The Python services use PostgreSQL for durable intake and RabbitMQ to dispatch work. The architecture has been designed/constructed to support reservation processing even in the event of failure

[Production operations](ops/PRODUCTION.md)

## Explore the architecture

From a local checkout, double-click [architecture.html](.archify/architecture-reservation-platform-20261125-220703/architecture.html) to open the viewer in your browser.



![Reservation ingestion architecture](.archify/architecture-reservation-platform-20261125-220703/architecture.svg)

## Run locally

Install a compose compatible container runtime. From the project directory, start the services and provision a partner key.

```bash
docker compose up --build -d
docker compose exec api reservation-ingest seed --rooms-per-property 2
```

Use the seeded `housing` UUID in reservation requests and supply an `Idempotency-Key` for each new submission. Try both endpoints directly in Swagger. The API returns a status URL for tracking each submission. [OpenAPI docs](http://localhost:8000/docs)

The seeder supports `--owners`, `--properties-per-owner`, and `--rooms-per-property`. Repeating the same command preserves identifiers. Requests default to the first seeded integration. For multi-tenant tests, use `X-Integration-Id` to select another integration on submission and status requests.

Stop the stack with `docker compose down`. Local data lives in named volumes. If your system provides Compose as `docker-compose`, use that command in place of `docker compose`.

## Development

```bash
uv sync
uv run ruff check .
uv run pyright
uv run pytest
```

The Compose stack exposes PostgreSQL on port 15432 and RabbitMQ on port 15673, both on localhost. Set `RESERVATION_TEST_URL=http://localhost:8000` and `RESERVATION_DATABASE_URL=postgresql+psycopg://reservations:reservations@localhost:15432/reservations` to include the integration suite against a running stack.
