-- +goose Up

-- The relay claims a batch of events with a lease instead of holding row
-- locks in an open transaction while it publishes. A claimed event is
-- invisible to other relays until claimed_until passes.
ALTER TABLE outbox ADD COLUMN claimed_until timestamptz NULL;

-- +goose Down
ALTER TABLE outbox DROP COLUMN claimed_until;
