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

# Format code
fmt:
    go fmt ./...

# Lint code
lint:
    go vet ./...

# Tidy dependencies
tidy:
    go mod tidy

# Create and push a new release tag (bumps patch version)
release:
    ./scripts/release.sh
