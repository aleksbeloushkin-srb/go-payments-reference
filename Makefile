GO      ?= go
PKGS    := ./...

PROMETHEUS_IMAGE ?= prom/prometheus:v3.5.0

.PHONY: run test test-integration e2e compare lint check-alerts compose-up compose-down

## run: build and start the whole stack in the foreground (Ctrl-C stops it)
run:
	docker compose up --build

## test: unit tests with the race detector (no Docker needed)
test:
	$(GO) test -race -count=1 $(PKGS)

## test-integration: integration tests against PostgreSQL in Docker
test-integration:
	$(GO) test -race -count=1 -tags integration $(PKGS)

## e2e: start the stack, drive it with curl, tear it down (START_MODE=in_tx for v1)
e2e:
	./scripts/e2e.sh

## compare: run both start modes under a slow gateway and print the metrics
compare:
	./scripts/compare-start-modes.sh

## lint: formatting, vet and staticcheck (if installed)
lint:
	@test -z "$$(gofmt -s -l .)" || { gofmt -s -l .; echo "run gofmt -s -w ."; exit 1; }
	$(GO) vet $(PKGS)
	$(GO) vet -tags integration $(PKGS)
	@if command -v staticcheck >/dev/null; then staticcheck -checks all $(PKGS); else echo "staticcheck not installed, skipped"; fi

## check-alerts: validate the Prometheus config and run the alert rule tests
check-alerts:
	docker run --rm -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" --entrypoint promtool \
		$(PROMETHEUS_IMAGE) check config /etc/prometheus/prometheus.yml
	docker run --rm -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" -w /etc/prometheus --entrypoint promtool \
		$(PROMETHEUS_IMAGE) test rules alerts_test.yml

## compose-up: start the stack in the background and wait until healthy
compose-up:
	docker compose up --build -d --wait

## compose-down: stop the stack and delete its volumes
compose-down:
	docker compose down -v
