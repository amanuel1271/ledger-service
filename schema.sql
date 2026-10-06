-- Money is stored in integer cents. The database, not app code, enforces the rules.
CREATE TABLE IF NOT EXISTS accounts (
    id      BIGSERIAL PRIMARY KEY,
    owner   TEXT   NOT NULL,
    balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0)
);

CREATE TABLE IF NOT EXISTS transfers (
    id              BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT   NOT NULL UNIQUE,
    from_id         BIGINT NOT NULL REFERENCES accounts,
    to_id           BIGINT NOT NULL REFERENCES accounts,
    amount          BIGINT NOT NULL CHECK (amount > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (from_id <> to_id)
);

-- Transactional outbox: events are written in the same transaction as the transfer,
-- so a committed transfer always has its events. The relay publishes them to Redis, then deletes them.
CREATE TABLE IF NOT EXISTS outbox (
    id         BIGSERIAL PRIMARY KEY,
    channel    TEXT  NOT NULL,
    payload    JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
