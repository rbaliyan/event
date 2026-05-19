# Container runtime (docker or podman). Picks whichever is on PATH; docker
# wins if both are present. Used by the `services-*` recipes to bring up
# Redis + Postgres for local integration testing.
CONTAINER := if `command -v docker >/dev/null 2>&1 && echo yes || echo no` == "yes" { "docker" } else { if `command -v podman >/dev/null 2>&1 && echo yes || echo no` == "yes" { "podman" } else { "" } }

# Default recipe
default:
    @just --list

# Build all packages
build:
    go build ./...

# Run tests
test:
    go test ./...

# Run tests with verbose output
test-v:
    go test -v ./...

# Run tests with race detector
test-race:
    go test -race ./...

# Run tests with coverage
test-cover:
    go test -cover ./...

# Run smoke suite (sub-100ms bus-level round-trips, build-tag gated)
test-smoke:
    go test -tags=smoke -race -count=1 -timeout=60s ./...

# Run integration suite (real backends; requires REDIS_ADDR / POSTGRES_DSN env or local services)
test-integration:
    go test -tags=integration -race -count=1 -timeout=300s ./...

# Run integration tests that talk to Redis
test-redis:
    go test -tags=integration -race -count=1 -run='.*[Rr]edis.*' ./...

# Run integration tests that talk to Postgres
test-pg:
    go test -tags=integration -race -count=1 -run='.*[Pp]ostgres.*' ./...

# Run the full unit suite with shuffled order to surface ordering-dependent tests
test-shuffle:
    go test -race -shuffle=on -count=1 ./...

# Format code
fmt:
    go fmt ./...

# Lint code
lint:
    golangci-lint run ./...

# Tidy dependencies
tidy:
    go mod tidy

# Run vulnerability check
vulncheck:
    go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# Check for outdated dependencies
depcheck:
    go list -m -u all | grep '\[' || echo "All dependencies are up to date"

# Create and push a new release tag (bumps patch version)
release:
    ./scripts/release.sh

# Start ephemeral Redis + Postgres containers (auto-detects docker or podman)
services-up:
    @if [ -z "{{CONTAINER}}" ]; then echo "neither docker nor podman found on PATH"; exit 1; fi
    @echo "using {{CONTAINER}}"
    {{CONTAINER}} run -d --rm --name event-test-redis -p 6379:6379 docker.io/library/redis:7-alpine
    {{CONTAINER}} run -d --rm --name event-test-postgres \
        -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test \
        -p 5432:5432 docker.io/library/postgres:16-alpine

# Stop the containers started by `services-up`.
services-down:
    @if [ -z "{{CONTAINER}}" ]; then echo "neither docker nor podman found on PATH"; exit 1; fi
    -{{CONTAINER}} stop event-test-redis
    -{{CONTAINER}} stop event-test-postgres
