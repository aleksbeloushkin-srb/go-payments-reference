package payment

import "regexp"

// Currency is an ISO 4217 alphabetic code, for example "EUR".
type Currency string

// supported lists the accepted currencies. Amounts are always integers in
// minor units (cents for EUR, yen for JPY), never floats.
var supported = map[Currency]bool{
	"EUR": true,
	"USD": true,
	"GBP": true,
	"CHF": true,
	"JPY": true,
}

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

// MaxAmountMinor caps a single payment: 10 million in major units of a
// two-decimal currency. It keeps arithmetic far away from int64 overflow and
// rejects obviously wrong input early.
const MaxAmountMinor int64 = 1_000_000_000

// Money is an amount in minor units together with its currency.
type Money struct {
	AmountMinor int64
	Currency    Currency
}

// NewMoney validates and builds a Money value.
func NewMoney(amountMinor int64, currency string) (Money, error) {
	if amountMinor <= 0 {
		return Money{}, &ValidationError{Field: "amount_minor", Reason: "must be positive"}
	}
	if amountMinor > MaxAmountMinor {
		return Money{}, &ValidationError{Field: "amount_minor", Reason: "exceeds the maximum"}
	}
	if !currencyPattern.MatchString(currency) {
		return Money{}, &ValidationError{Field: "currency", Reason: "must be a 3-letter ISO 4217 code"}
	}
	c := Currency(currency)
	if !supported[c] {
		return Money{}, &ValidationError{Field: "currency", Reason: "is not supported"}
	}
	return Money{AmountMinor: amountMinor, Currency: c}, nil
}
