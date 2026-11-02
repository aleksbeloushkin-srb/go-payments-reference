package ports

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// OutboxRecord is a stored integration event waiting to be published.
type OutboxRecord struct {
	ID        int64
	PaymentID uuid.UUID
	Type      string
	Payload   json.RawMessage
	CreatedAt time.Time
}
