"""Load test: fire concurrent transfers at a running ledger-service, then check no money was created or lost.

    python loadtest.py --requests 5000 --concurrency 50              # spread over 100 accounts
    python loadtest.py --requests 5000 --concurrency 50 --hot        # every transfer leaves account #1
"""
import argparse
import asyncio
import random
import statistics
import sys
import time
import uuid
from collections import Counter

import httpx


async def run(url: str, requests: int, concurrency: int, n_accounts: int, hot: bool) -> bool:
    async with httpx.AsyncClient(base_url=url, timeout=30, limits=httpx.Limits(max_connections=concurrency)) as http:
        ids = []
        for _ in range(n_accounts):
            r = await http.post("/accounts", json={"owner": "load", "opening_balance": 1_000_000})
            r.raise_for_status()
            ids.append(r.json()["id"])

        async def total() -> int:
            balances = await asyncio.gather(*(http.get(f"/accounts/{i}") for i in ids))
            return sum(b.json()["balance"] for b in balances)

        before = await total()
        latencies, statuses = [], Counter()
        remaining = iter(range(requests))

        async def worker():
            for _ in remaining:  # shared iterator: workers pull until all requests are sent
                src, dst = random.sample(ids, 2)
                if hot:
                    src, dst = ids[0], random.choice(ids[1:])
                start = time.perf_counter()
                try:
                    r = await http.post(
                        "/transfers",
                        json={"from_id": src, "to_id": dst, "amount": random.randint(1, 100)},
                        headers={"Idempotency-Key": str(uuid.uuid4())},
                    )
                    statuses[r.status_code] += 1
                except httpx.HTTPError as e:
                    statuses[type(e).__name__] += 1
                latencies.append(time.perf_counter() - start)

        started = time.perf_counter()
        await asyncio.gather(*(worker() for _ in range(concurrency)))
        elapsed = time.perf_counter() - started
        after = await total()

    ms = statistics.quantiles([x * 1000 for x in latencies], n=100)
    print(f"scenario     {'hot account' if hot else f'spread over {n_accounts} accounts'}")
    print(f"requests     {requests} at concurrency {concurrency} in {elapsed:.2f}s")
    print(f"throughput   {requests / elapsed:.0f} transfers/s")
    print(f"latency ms   p50 {ms[49]:.1f}   p95 {ms[94]:.1f}   p99 {ms[98]:.1f}   max {max(latencies) * 1000:.1f}")
    print(f"statuses     {dict(statuses)}")
    conserved = before == after
    print(f"money        {'conserved' if conserved else f'NOT conserved: {before} -> {after}'}")
    return conserved and set(statuses) == {201}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://localhost:8000")
    p.add_argument("--requests", type=int, default=2000)
    p.add_argument("--concurrency", type=int, default=50)
    p.add_argument("--accounts", type=int, default=100)
    p.add_argument("--hot", action="store_true", help="every transfer debits the same account (worst-case lock contention)")
    a = p.parse_args()
    sys.exit(0 if asyncio.run(run(a.url, a.requests, a.concurrency, a.accounts, a.hot)) else 1)
