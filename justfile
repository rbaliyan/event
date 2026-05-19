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
