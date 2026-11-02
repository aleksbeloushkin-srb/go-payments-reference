package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

func TestWaitPaymentReturnsAtOnceWhenAlreadyNewer(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusProcessing)
	p.Version = 3
	f.store.payments[p.ID] = p

	res, err := f.svc.WaitPayment(context.Background(), p.ID, 2)

	if err != nil || !res.Modified || res.Payment.Version != 3 {
		t.Fatalf("res = %+v, err = %v", res, err)
	}
	if !f.watcher.canceled {
		t.Fatal("the waiter must be deregistered")
	}
}

func TestWaitPaymentWakesOnChange(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusProcessing)
	f.store.readDone = make(chan struct{}, 2)

	done := make(chan WaitResult)
	go func() {
		res, err := f.svc.WaitPayment(context.Background(), p.ID, p.Version)
		if err != nil {
			t.Error(err)
		}
		done <- res
	}()

	// Wait until the first read has seen the old version, so the wake-up
	// path is the one under test. Then commit the change before the wake-up,
	// as the real batcher would only see it after the commit.
	<-f.store.readDone
	f.store.mu.Lock()
	changed := f.store.payments[p.ID]
	changed.Status, changed.Version = payment.StatusSucceeded, p.Version+1
	f.store.payments[p.ID] = changed
	f.store.mu.Unlock()
	f.watcher.ch <- p.Version + 1

	res := <-done
	if !res.Modified || res.Payment.Status != payment.StatusSucceeded {
		t.Fatalf("res = %+v", res)
	}
}

func TestWaitPaymentSoftTimeoutIsNotAnError(t *testing.T) {
	cfg := testConfig
	cfg.WaitTimeout = time.Millisecond
	f := newFixture(cfg)
	p := f.seed(payment.StatusProcessing)

	res, err := f.svc.WaitPayment(context.Background(), p.ID, p.Version)

	if err != nil || res.Modified || res.Payment.Version != p.Version {
		t.Fatalf("res = %+v, err = %v; want not modified", res, err)
	}
}

func TestWaitPaymentClientGoneReturnsContextError(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusProcessing)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := f.svc.WaitPayment(ctx, p.ID, p.Version)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestWaitPaymentShutdownAnswersNotModified(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusProcessing)
	close(f.watcher.ch)

	res, err := f.svc.WaitPayment(context.Background(), p.ID, p.Version)

	if err != nil || res.Modified {
		t.Fatalf("res = %+v, err = %v; want not modified", res, err)
	}
}

func TestWaitPaymentFullRegistryRejectsBeforeReadingTheDatabase(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusProcessing)
	f.watcher.err = ports.ErrTooManyWaiters

	_, err := f.svc.WaitPayment(context.Background(), p.ID, 1)

	if !errors.Is(err, ports.ErrTooManyWaiters) {
		t.Fatalf("err = %v, want ErrTooManyWaiters", err)
	}
	if f.store.reads != 0 {
		t.Fatalf("store reads = %d, want 0", f.store.reads)
	}
}
