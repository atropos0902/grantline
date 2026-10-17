.PHONY: up down build test integration leaks known-bugs checker console gen campaigns drill

up:            ## start everything, then open http://localhost:8080
	docker compose up --build -d --wait --wait-timeout 180 primary standby target
	docker compose up -d
	@echo "console: http://localhost:8080"

down:
	docker compose down -v

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/ ./cmd/grantline ./cmd/campaign ./cmd/loadgen

test:
	go vet ./... && go test -race ./internal/...
	cd operator && go vet ./... && go test ./...

integration:   ## needs Docker
	scripts/it.sh

leaks:         ## the integration tests with the goroutine leak profile
	GOEXPERIMENT=goroutineleakprofile go test ./test/integration -count=1 -v | grep -E '^(---|ok|FAIL|leak)'

known-bugs:    ## every known bug has to be caught by its test
	scripts/known-bugs.sh

checker:
	cd checker && uv run --python 3.14 pytest -q

console:
	cd console && npm ci && npm run build && npm test

gen:
	cd console && npm ci
	buf lint && buf generate

campaigns: build
	for c in races kills pauses failover lag lanes copy bigtx; do bin/campaign $$c || exit 1; done

drill: build   ## a support ticket to solve with docs/runbook.md
	go run -tags bug_log_position_guard ./cmd/campaign drill
