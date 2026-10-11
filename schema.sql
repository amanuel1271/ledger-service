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

-- Deposits (+) and withdrawals (-). Idempotent like transfers.
CREATE TABLE IF NOT EXISTS cash_movements (
    id              BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT   NOT NULL UNIQUE,
    account_id      BIGINT NOT NULL REFERENCES accounts,
    amount          BIGINT NOT NULL CHECK (amount <> 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The ledger: one row per balance change, written in the same transaction as the change.
-- Invariant (tested): accounts.balance = SUM(entries.amount) for every account.
CREATE TABLE IF NOT EXISTS entries (
    id         BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES accounts,
    amount     BIGINT NOT NULL CHECK (amount <> 0),
    kind       TEXT   NOT NULL CHECK (kind IN ('opening', 'deposit', 'withdrawal', 'transfer_in', 'transfer_out')),
    ref_id     BIGINT,  -- transfers.id or cash_movements.id
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS entries_account_newest ON entries (account_id, id DESC);  -- history pages

-- Account age is a risk signal. ALTER (not the CREATE above) so existing databases get it too.
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Cache of model verdicts, so a transfer is sent to the model once. Rules verdicts are not
-- cached: they are instant, and the model gets another chance next time.
CREATE TABLE IF NOT EXISTS risk_reviews (
    transfer_id BIGINT PRIMARY KEY REFERENCES transfers,
    risk        TEXT NOT NULL CHECK (risk IN ('low', 'medium', 'high')),
    reason      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
