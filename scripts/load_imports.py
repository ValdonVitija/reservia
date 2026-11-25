import argparse
import asyncio
import statistics
import time
from dataclasses import dataclass
from uuid import uuid4

import httpx


@dataclass(frozen=True, slots=True)
class Submission:
    housing: str
    lane: str
    integration_id: str | None
    size: int


@dataclass(frozen=True, slots=True)
class Measurement:
    lane: str
    rows: int
    accepted_ms: float
    accepted_at: float
    completed_at: float
    retries: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load test a running reservation API")
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--housing", required=True)
    parser.add_argument("--other-housing", required=True)
    parser.add_argument("--integration-id")
    parser.add_argument("--other-integration-id", required=True)
    parser.add_argument("--rows", type=int, default=60_000)
    return parser.parse_args()


async def send(
    client: httpx.AsyncClient, request: Submission, slots: asyncio.Semaphore
) -> Measurement:
    path = "/v1/reservation-imports" if request.lane != "single" else "/v1/reservations"
    rows = [
        {
            "housing": request.housing,
            "external_id": f"load-{uuid4()}",
            "check_in_date": "2027-06-12",
            "check_out_date": "2027-06-15",
            "guest_group": {"known_number_of_guests": 2},
        }
        for _ in range(request.size)
    ]
    body = rows[0] if request.lane == "single" else {"reservations": rows}
    scope = (
        {"X-Integration-Id": request.integration_id} if request.integration_id else {}
    )
    headers = scope | {"Idempotency-Key": uuid4().hex}
    start = time.monotonic()
    retries = 0
    while True:
        async with slots:
            if retries == 0:
                start = time.monotonic()
            response = await client.post(path, json=body, headers=headers)
        if response.status_code in {200, 202}:
            break
        if response.status_code not in {429, 503} or retries == 6:
            response.raise_for_status()
            raise RuntimeError(f"unexpected intake status {response.status_code}")
        await asyncio.sleep(float(response.headers.get("Retry-After", "1")))
        retries += 1
    accepted = time.monotonic()

    async with asyncio.timeout(240):
        while True:
            async with slots:
                status = await client.get(
                    response.headers["Location"],
                    params={"limit": 1},
                    headers=scope,
                )
            status.raise_for_status()
            counts = status.json()
            if not counts["pending"]:
                if (
                    counts["succeeded"] != request.size
                    or counts["rejected"]
                    or counts["failed"]
                ):
                    raise RuntimeError("import did not reconcile to the input size")
                break
            await asyncio.sleep(0.5)
    return Measurement(
        request.lane,
        request.size,
        (accepted - start) * 1000,
        accepted,
        time.monotonic(),
        retries,
    )


async def run(args: argparse.Namespace) -> None:
    if args.rows <= 0:
        raise ValueError("--rows must be positive")
    main = [
        Submission(
            args.housing, "bulk", args.integration_id, min(500, args.rows - start)
        )
        for start in range(0, args.rows, 500)
    ]
    slots_by_lane = {
        "bulk": asyncio.Semaphore(12),
        "other": asyncio.Semaphore(2),
        "single": asyncio.Semaphore(2),
    }

    async with httpx.AsyncClient(base_url=args.url, timeout=10) as client:
        start = time.monotonic()
        active_bulk = [
            asyncio.create_task(send(client, request, slots_by_lane["bulk"]))
            for request in main
        ]

        async def additional_traffic(
            lane: str, request: Submission, max_calls: int
        ) -> list[Measurement]:
            records = []
            for _ in range(max_calls):
                if records and all(task.done() for task in active_bulk):
                    break
                records.append(await send(client, request, slots_by_lane[lane]))
            return records

        other_task = asyncio.create_task(
            additional_traffic(
                "other",
                Submission(args.other_housing, "other", args.other_integration_id, 100),
                100,
            )
        )
        single_task = asyncio.create_task(
            additional_traffic(
                "single",
                Submission(args.housing, "single", args.integration_id, 1),
                100,
            )
        )
        primary = await asyncio.gather(*active_bulk)
        other, singles = await asyncio.gather(other_task, single_task)

    for lane, records in (("bulk", primary), ("other", other), ("single", singles)):
        latest_accepted = max(item.accepted_at for item in records)
        latest_completed = max(item.completed_at for item in records)
        times = [item.accepted_ms for item in records]
        p95 = (
            statistics.quantiles(times, n=20, method="inclusive")[18]
            if len(times) > 1
            else times[0]
        )
        rows = sum(item.rows for item in records)
        print(
            f"{lane}: {rows} rows / {len(records)} requests; "
            f"{sum(item.retries for item in records)} retries; "
            f"p95 acceptance {p95:.1f} ms; "
            f"{rows / (latest_completed - start):.1f} rows/s through completion; "
            f"drain after last 202 {latest_completed - latest_accepted:.2f}s"
        )


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
