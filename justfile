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
    #!/usr/bin/env bash
    set -euo pipefail
    latest=$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
    echo "Current version: $latest"
    version=${latest#v}
    IFS='.' read -r major minor patch <<< "$version"
    new_patch=$((patch + 1))
    new_version="v${major}.${minor}.${new_patch}"
    echo "New version: $new_version"
    git tag -a "$new_version" -m "Release $new_version"
    git push origin "$new_version"
    echo "Released $new_version"
