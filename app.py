import asyncio
import contextlib
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, Header, HTTPException, WebSocket
from pydantic import BaseModel, Field

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://ledger:ledger@localhost:55432/ledger")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:56379")
log = logging.getLogger("ledger")


class OutboxRelay:
    """Moves events from the outbox table to Redis. Owns its background task, so start/stop/pause live in one place."""

    def __init__(self, db, redis_client, batch: int = 100, idle_s: float = 0.1):
        self.db, self.redis, self.batch, self.idle_s = db, redis_client, batch, idle_s
        self.paused = False
        self._task = None

    def start(self):
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task  # let an in-flight batch roll back cleanly before the pool closes

    def pause(self):  # tests pause it to inspect the outbox
        self.paused = True

    def resume(self):
        self.paused = False

    async def run_once(self) -> int:
        """Publish one batch, then delete it. If publishing fails the transaction rolls back
        and the events stay for the next try (at-least-once delivery)."""
        async with self.db.acquire() as conn, conn.transaction():
            rows = await conn.fetch(
                "SELECT id, channel, payload FROM outbox ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED", self.batch
            )
            for row in rows:  # event_id lets consumers drop the rare duplicate after a crash mid-batch
                await self.redis.publish(row["channel"], json.dumps({**json.loads(row["payload"]), "event_id": row["id"]}))
            if rows:
                await conn.execute("DELETE FROM outbox WHERE id = ANY($1::bigint[])", [r["id"] for r in rows])
        return len(rows)

    async def _run(self):
        # ponytail: one poller per process, 100 ms idle latency. Switch to LISTEN/NOTIFY if that's too slow.
        while True:
            sent = 0
            if not self.paused:
                try:
                    sent = await self.run_once()
                except Exception:
                    log.exception("[Outbox] RELAY_FAIL retry_in=1s")
                    await asyncio.sleep(1)
            await asyncio.sleep(0 if sent == self.batch else self.idle_s)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await asyncpg.create_pool(DATABASE_URL)
    app.state.redis = redis.from_url(REDIS_URL, decode_responses=True)
    await app.state.db.execute((Path(__file__).parent / "schema.sql").read_text())
    app.state.relay = OutboxRelay(app.state.db, app.state.redis)
    app.state.relay.start()
    yield
    await app.state.relay.stop()
    await app.state.db.close()
    await app.state.redis.aclose()


app = FastAPI(title="ledger-service", lifespan=lifespan)


class NewAccount(BaseModel):
    owner: str = Field(min_length=1)
    opening_balance: int = Field(0, ge=0)


class NewTransfer(BaseModel):
    from_id: int
    to_id: int
    amount: int = Field(gt=0)


@app.post("/accounts", status_code=201)
async def create_account(body: NewAccount):
    row = await app.state.db.fetchrow(
        "INSERT INTO accounts (owner, balance) VALUES ($1, $2) RETURNING *",
        body.owner, body.opening_balance,
    )
    return dict(row)


@app.get("/accounts/{account_id}")
async def get_account(account_id: int):
    row = await app.state.db.fetchrow("SELECT * FROM accounts WHERE id = $1", account_id)
    if not row:
        raise HTTPException(404, "account not found")
    return dict(row)


@app.post("/transfers", status_code=201)
async def create_transfer(body: NewTransfer, idempotency_key: str = Header(min_length=1)):
    if body.from_id == body.to_id:
        raise HTTPException(422, "cannot transfer to the same account")
    try:
        async with app.state.db.acquire() as conn, conn.transaction():
            # Claim the key first. A concurrent duplicate blocks here on the unique
            # index until we commit, then gets nothing back and replays below.
            transfer = await conn.fetchrow(
                """INSERT INTO transfers (idempotency_key, from_id, to_id, amount)
                   VALUES ($1, $2, $3, $4) ON CONFLICT (idempotency_key) DO NOTHING
                   RETURNING *""",
                idempotency_key, body.from_id, body.to_id, body.amount,
            )
            if transfer is None:
                return await replay(conn, idempotency_key, body)
            # Update in id order so two opposite transfers can't deadlock.
            # The balance >= 0 CHECK rejects overdrafts atomically.
            for account_id, delta in sorted([(body.from_id, -body.amount), (body.to_id, body.amount)]):
                await conn.execute(
                    "UPDATE accounts SET balance = balance + $1 WHERE id = $2", delta, account_id
                )
            # Same transaction: if the transfer commits, its events exist; if it rolls back, they don't.
            event = json.dumps({"type": "transfer", **dict(transfer), "created_at": transfer["created_at"].isoformat()})
            await conn.executemany(
                "INSERT INTO outbox (channel, payload) VALUES ($1, $2::jsonb)",
                [(f"account:{account_id}", event) for account_id in (body.from_id, body.to_id)],
            )
    except asyncpg.ForeignKeyViolationError:
        raise HTTPException(404, "account not found")
    except asyncpg.CheckViolationError:
        raise HTTPException(422, "insufficient funds")
    return dict(transfer)


async def replay(conn, idempotency_key: str, body: NewTransfer):
    existing = await conn.fetchrow("SELECT * FROM transfers WHERE idempotency_key = $1", idempotency_key)
    if (existing["from_id"], existing["to_id"], existing["amount"]) != (body.from_id, body.to_id, body.amount):
        raise HTTPException(409, "idempotency key reused with a different request")
    return dict(existing)


@app.websocket("/ws/accounts/{account_id}")
async def watch_account(ws: WebSocket, account_id: int):
    await ws.accept()
    async with app.state.redis.pubsub() as pubsub:
        await pubsub.subscribe(f"account:{account_id}")
        await ws.send_json({"type": "subscribed", "account_id": account_id})
        async for message in pubsub.listen():
            if message["type"] == "message":
                await ws.send_text(message["data"])
