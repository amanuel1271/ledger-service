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
