-- +goose Up

-- One row per call the service makes, or is about to make, to create a
-- payment at the gateway (two-phase start). The row is committed before the
-- call, so after any crash the database says which call may have happened
-- and with which idempotency key.
CREATE TABLE gateway_attempts (
    payment_id      uuid        NOT NULL REFERENCES payments (id),
    attempt         integer     NOT NULL CHECK (attempt > 0),
    idempotency_key text        NOT NULL,
    state           text        NOT NULL CHECK (state IN ('intent', 'done', 'unknown', 'abandoned')),
    gateway_ref     text        NULL,
    -- Certain "not found" answers seen by reconcile for this attempt.
    not_found       integer     NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,
    PRIMARY KEY (payment_id, attempt),
    CONSTRAINT gateway_attempts_idempotency_key_key UNIQUE (idempotency_key)
);

-- A payment has at most one open attempt; the database enforces it.
CREATE UNIQUE INDEX gateway_attempts_one_open_idx ON gateway_attempts (payment_id)
    WHERE state IN ('intent', 'unknown');

-- The metrics collector counts open attempts and the age of the oldest one
-- on every scrape. Done and abandoned attempts are terminal and only grow,
-- so they are kept out of the index; abandons are counted by
-- reconcile_total.
CREATE INDEX gateway_attempts_open_age_idx ON gateway_attempts (created_at)
    WHERE state IN ('intent', 'unknown');

-- +goose Down
DROP TABLE gateway_attempts;
