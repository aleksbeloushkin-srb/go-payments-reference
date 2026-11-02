-- +goose Up

-- The metrics collector counts ready and dead tasks by kind on every scrape.
-- This partial index keeps that count an index scan as the table grows.
CREATE INDEX tasks_state_kind_idx ON tasks (state, kind) WHERE state IN ('ready', 'dead');

-- +goose Down
DROP INDEX tasks_state_kind_idx;
