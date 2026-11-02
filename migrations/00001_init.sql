-- +goose Up

-- One row per payment. version is the change marker used by long polling and
-- by event consumers; it grows by one on every status change.
CREATE TABLE payments (
    id              uuid        PRIMARY KEY,
    merchant_id     text        NOT NULL,
    amount_minor    bigint      NOT NULL CHECK (amount_minor > 0),
    currency        text        NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    status          text        NOT NULL,
    version         bigint      NOT NULL CHECK (version > 0),
    gateway_ref     text        NULL,
    idempotency_key text        NOT NULL,
    request_hash    text        NOT NULL,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,
    CONSTRAINT payments_idempotency_key_key UNIQUE (idempotency_key)
);

-- Background commands. A task is claimed by setting lease_until; it becomes
-- claimable again when the lease expires. (kind, payment_id, dedup_key) makes
-- enqueueing idempotent.
CREATE TABLE tasks (
    id          bigserial   PRIMARY KEY,
    kind        text        NOT NULL,
    payment_id  uuid        NOT NULL,
    dedup_key   text        NOT NULL,
    payload     jsonb       NOT NULL DEFAULT '{}',
    attempts    integer     NOT NULL DEFAULT 0,
    run_after   timestamptz NOT NULL,
    lease_until timestamptz NULL,
    last_error  text        NULL,
    state       text        NOT NULL DEFAULT 'ready' CHECK (state IN ('ready', 'done', 'dead')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT tasks_dedup_key UNIQUE (kind, payment_id, dedup_key)
);

-- The claim query scans only ready tasks in run_after order.
CREATE INDEX tasks_ready_idx ON tasks (run_after, id) WHERE state = 'ready';

-- Integration events written in the same transaction as the change they
-- describe. The relay publishes them and sets published_at.
CREATE TABLE outbox (
    id           bigserial   PRIMARY KEY,
    payment_id   uuid        NOT NULL,
    type         text        NOT NULL,
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL,
    published_at timestamptz NULL
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox;
DROP TABLE tasks;
DROP TABLE payments;
