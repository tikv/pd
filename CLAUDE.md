# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PD (Placement Driver) is the cluster manager for TiKV. It handles metadata storage, timestamp allocation (TSO), and scheduling decisions for TiKV clusters. PD embeds etcd for fault-tolerance and leader election.

## Build Commands

```bash
make                    # Build pd-server, pd-ctl, pd-recover
make pd-server          # Build only pd-server
make pd-server-basic    # Build without dashboard (SWAGGER=0 DASHBOARD=0, faster)
make tools              # Build benchmark and utility tools
make install-tools      # Install dev tools to .tools/bin (golangci-lint v2.6.0, etc.)

# Build variants
WITH_RACE=1 make build        # Enable race detector (CGO on)
SWAGGER=1 make build          # Generate swagger spec
ENABLE_FIPS=1 make build      # FIPS/boringcrypto build
NEXT_GEN=1 make build         # NextGen mode (disables dashboard)
make simulator                # Build pd-simulator
make docker-image             # Build Docker image
```

**Go version requirement: >=1.25**

## Testing

```bash
make test               # Full suite with race detection, deadlock tag, failpoints
make basic-test         # Fast tests without race (excludes tests/ packages)
make ut                 # Unit tests only via pd-ut binary (excludes tests/ directory)
make test-tso-function  # TSO-specific tests

# Run a single test (uses gocheck)
go test github.com/tikv/pd/server/api -check.f TestJsonRespondError
go test ./pkg/schedule/... -run TestSpecificTest

# Integration tests (in tests/integrations/)
cd tests/integrations && make test test_name=client
cd tests/integrations && make test test_name=mcs

# Real cluster tests
make test-real-cluster  # Uses tiup, wipes ~/.tiup/data/pd_real_cluster_test

# Cleanup
make clean-test         # Remove /tmp/pd_tests*, test cache, UT binaries
```

### Failpoints

Tests use failpoints which must be enabled/disabled properly:
- `make failpoint-enable` / `make failpoint-disable`
- Make targets auto-enable/disable; if running `go test` manually, bracket with enable/disable
- Never commit with failpoints enabled; verify `git status` is clean before pushing

## Linting

```bash
make check              # Full check: tidy + static + generate-errdoc
make static             # gofmt -s + golangci-lint + leakcheck
make tidy               # go mod tidy (CI enforces clean diff)
make fmt                # gofmt with interface{} -> any rewrite
make generate-errdoc    # Regenerate errors.toml
make generate-easyjson  # Update pkg/response/region.go
```

## Architecture

### Multi-module Structure

The project has multiple Go modules with separate go.mod files:
- Root module (`github.com/tikv/pd`)
- `client/` - PD client library (run `make` from client/ for its pipeline)
- `tools/` - CLI tools and utilities
- `tests/integrations/` - Integration tests

### Core Components

**Server (`server/`):**
- `api/` - REST API v1 endpoints
- `apiv2/` - REST API v2 with modern handlers
- `cluster/` - Cluster state and region management
- `grpc_service.go` - gRPC service implementation

**Scheduling (`pkg/schedule/`):**
- `coordinator.go` - Main scheduling loop
- `schedulers/` - 20+ scheduler implementations (balance-leader, balance-region, etc.)
- `checker/` - Data integrity checkers (replica, merge, rule)
- `operator/` - Scheduling operations execution
- `placement/` - Placement rules engine

**TSO (`pkg/tso/`):**
- Timestamp Oracle for distributed transaction ordering
- Keyspace group management for multi-tenancy

**Core Data Structures (`pkg/core/`):**
- `BasicCluster` - Cluster metadata container
- `RegionsInfo` - Region tree and index management
- `StoresInfo` - Store (TiKV node) information

**Storage (`pkg/storage/`):**
- Backend abstraction (etcd, LevelDB, memory)
- Endpoint interfaces for different data types

### Microservices Mode (`pkg/mcs/`)

PD supports running services as separate microservices:
- `tso/` - Distributed TSO service
- `scheduling/` - Scheduling service
- `resourcemanager/` - Resource group management

Start microservices: `pd-server services <api|tso|scheduling|resource-manager>`

### Client Library (`client/`)

- `client.go` - Main PD client for TiKV communication
- `clients/tso/` - TSO client
- `servicediscovery/` - Service discovery
- `resource_group/` - Resource management client

## Code Conventions

### Imports and Formatting
- Import order (gci enforced): standard | third-party | `github.com/pingcap` | `github.com/tikv/pd` | blank
- Run `make fmt` for gofmt with `interface{}` -> `any` rewrite
- Avoid dot-imports (revive warns)

### Banned Packages (depguard enforced)
- Use `github.com/pingcap/errors` (not `github.com/pkg/errors`)
- Use `sync/atomic` (not `go.uber.org/atomic`)
- Use `math/rand/v2` (not `math/rand`)

### Error Handling
- Wrap errors with `errors.Wrap`/`errors.Annotate` or `fmt.Errorf("...: %w", err)`
- Error strings: lowercase, no trailing punctuation
- HTTP handlers: use errcode + `errorResp` (avoid `http.Error`)
- Check errors with `errors.Is`/`errors.As` for sentinel errors

### Style
- First parameter `context.Context` for functions with external effects; never store in structs
- Acronyms uppercase: TSO, API, HTTP
- `sync.WaitGroup` must be passed as pointer (revive rule)
- Pointers for large structs; avoid pointer-to-interface
- All files require Apache 2.0 license header (goheader enforced)

### Concurrency
- Cancel timers/tickers; close resources with defer
- Guard shared state with mutex; keep lock ordering consistent
- Prevent goroutine leaks: pair with cancellation; consider errgroup

## API and Swagger

```bash
SWAGGER=1 make build    # Regenerate swagger spec
make swagger-spec       # Generate docs/swagger/ from annotations
make generate-easyjson  # Update easyjson for pkg/response/region.go
```

Update Go annotations when changing APIs. Format reference: [swaggo/swag](https://github.com/swaggo/swag#declarative-comments-format)

## Commit Requirements

- Sign off commits: `git commit -s -m "message"`
- PR title format: `pkg: description` (under 70 chars); multi-area use commas or `*:`
- PRs must include: `Issue Number: close #123` or `Issue Number: ref #456`
- Body wrapped at 80 chars

## Pre-PR Checklist

1. Run `make check` (or at minimum `make basic-test` for touched packages)
2. Ensure imports ordered, gofmt clean, modules tidy
3. Verify `git status` clean (no failpoint artifacts, generated files)
4. Fix all depguard, revive, testifylint findings before submission
