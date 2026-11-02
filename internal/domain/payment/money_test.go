package payment

import (
	"errors"
	"testing"
)

func TestNewMoney(t *testing.T) {
	tests := []struct {
		name      string
		amount    int64
		currency  string
		wantField string
	}{
		{name: "ok", amount: 1250, currency: "EUR"},
		{name: "zero-decimal currency", amount: 1, currency: "JPY"},
		{name: "zero amount", amount: 0, currency: "EUR", wantField: "amount_minor"},
		{name: "negative amount", amount: -5, currency: "EUR", wantField: "amount_minor"},
		{name: "too large", amount: MaxAmountMinor + 1, currency: "EUR", wantField: "amount_minor"},
		{name: "lowercase currency", amount: 1, currency: "eur", wantField: "currency"},
		{name: "unsupported currency", amount: 1, currency: "XXX", wantField: "currency"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewMoney(tt.amount, tt.currency)
			if tt.wantField == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if m.AmountMinor != tt.amount || string(m.Currency) != tt.currency {
					t.Fatalf("got %+v", m)
				}
				return
			}
			var verr *ValidationError
			if !errors.As(err, &verr) {
				t.Fatalf("err = %v, want *ValidationError", err)
			}
			if verr.Field != tt.wantField {
				t.Fatalf("field = %q, want %q", verr.Field, tt.wantField)
			}
		})
	}
}
