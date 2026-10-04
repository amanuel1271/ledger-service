import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, Header, HTTPException, WebSocket
from pydantic import BaseModel, Field

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://ledger:ledger@localhost:55432/ledger")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:56379")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await asyncpg.create_pool(DATABASE_URL)
    app.state.redis = redis.from_url(REDIS_URL, decode_responses=True)
    await app.state.db.execute((Path(__file__).parent / "schema.sql").read_text())
    yield
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
    except asyncpg.ForeignKeyViolationError:
        raise HTTPException(404, "account not found")
    except asyncpg.CheckViolationError:
        raise HTTPException(422, "insufficient funds")

    event = json.dumps({"type": "transfer", **dict(transfer), "created_at": transfer["created_at"].isoformat()})
    # ponytail: published after commit, so a crash right here drops the live event
    # (the ledger itself is safe). Use a transactional outbox if events must be durable.
    for account_id in (body.from_id, body.to_id):
        await app.state.redis.publish(f"account:{account_id}", event)
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
