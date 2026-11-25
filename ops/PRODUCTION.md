# Production operations

Deploy the API, relay, and bulk and individual workers from the same image as independently scalable services. Use a TLS load balancer with at least two API replicas, a managed PostgreSQL primary with automatic failover and point-in-time backups, and a three-node RabbitMQ cluster across distinct failure domains. Run Alembic once before starting application replicas. The local Compose credentials and exposed ports are for development.

## Configuration

- Inject connection URLs using the deployment secret store. Use the PostgreSQL writer endpoint and a RabbitMQ endpoint that routes reconnects to healthy cluster nodes. Use TLS and credentials scoped to each runtime role. Give migrations a separate database credential.
- Set database pool sizes per replica and reserve a separate allocation for individual workers. The total of all pools must fit the PostgreSQL connection limit with room for maintenance.
- Provision a tenant and housing with `reservation-ingest add-tenant` and `add-housing`. Pass `--room` for each room number. Use `add-integration --tenant-id TENANT_ID --name pms` to provision an integration. The `X-Integration-Id` header selects an integration for scoping imports and receipts.
- Apply the RabbitMQ policy before starting workers. Substitute the configured application vhost if needed.

```bash
rabbitmqctl set_policy -p / --apply-to quorum_queues --priority 10 \
  reserve-dead-letters '^reservations\.(bulk|single)$' \
  '{"dead-letter-strategy":"at-least-once","overflow":"reject-publish"}'
```

## Admission and recovery

- Tune the `RESERVATION_TENANT_*_ROWS_PER_MINUTE`, `RESERVATION_TENANT_*_BACKLOG_ROWS`, and shared `RESERVATION_*_BACKLOG_ROWS` settings for the `BULK` and `SINGLE` lanes. Apply the same values to all replicas. Per-tenant backlog caps preserve shared capacity. Each shared budget is split across 16 shards.
- Monitor `reservation-ingest stats`, HTTP acceptance latency, response codes, database pool saturation, and broker resource alarms. Alert on a growing oldest-unpublished or oldest-pending age, sustained 503 responses, or dead letters. Budget capacity for draining work while fresh requests continue.
- When an import is overdue, inspect the job ID and dependency health, then run `reservation-ingest replay JOB_ID` after correcting the cause. Replay replaces the dispatch generation and preserves the original receipt. Run `reservation-ingest prune` periodically to clear staged payloads for terminal imports outside the replay window. Set the retention period to the deployment's data policy.
- Return `503` during writer failover. Partners should retry the same request with the same `Idempotency-Key`. Existing work resumes when the writer returns. Broker unavailability can be absorbed up to the backlog budget.

## Release verification

Use a separate test stack to run the integration suite. With the test database and broker URLs and Compose project configured, run `uv run python scripts/verify_failures.py --project TEST_PROJECT`. It stops and restarts the test database and broker and exercises the publish and acknowledgement crash windows.

Run `scripts/load_imports.py` with `--housing`, `--other-housing`, and `--other-integration-id`. Use `--integration-id` to select a primary integration in place of the first seeded one. The harness sends 60,000 bookings to one property by default, keeps a second tenant and the individual endpoint active, and waits for every accepted booking to reconcile.

Before rollout, test RabbitMQ leader loss and PostgreSQL writer failover with traffic running. Restore a backup into an isolated environment and reconcile pending jobs with the saved intake receipts. Record acceptance latency, completion throughput, recovery time, and recovery point for that deployment.
