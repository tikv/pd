# GitHub Copilot Instructions for PD

This file provides guidance to GitHub Copilot when working with the PD (Placement Driver) codebase.

## Repository Overview

PD is the Placement Driver for TiKV clusters, written in Go. It manages and schedules TiKV clusters with fault-tolerance through embedded etcd.

**Key Technologies:**
- Language: Go 1.25+
- Framework: etcd, gRPC, Gin (HTTP server)
- Build System: Make
- Testing: Go test with race detection and failpoints

## Project Structure

- `/cmd` - Command-line tools (pd-server, pd-ctl, pd-recover)
- `/server` - Core PD server implementation
- `/pkg` - Shared packages and utilities
- `/client` - Go client library (separate submodule)
- `/tests` - Integration and unit tests
- `/tools` - Development tools and utilities
- `/metrics` - Prometheus metrics definitions
- `/scripts` - Build and deployment scripts

## Build and Development

### Essential Commands
```bash
make build           # Build pd-server, pd-ctl, pd-recover
make dev             # Full dev loop (build + check + tools + test)
make dev-basic       # Lightweight validation (build + check + basic-test)
make check           # Lint and static analysis (tidy + static + generate-errdoc)
make test            # Full test suite with race detection
make basic-test      # Fast tests without race detection
```

### Before Submitting Code
1. Run `make check` to lint and validate code
2. Run `make basic-test` for changed packages
3. Ensure `go.mod` and `go.sum` are clean with `make tidy`
4. Run `make fmt` to format code

## Code Style Guidelines

### Go Conventions
- **Imports ordering** (enforced by gci):
  1. Standard library
  2. Third-party packages
  3. `github.com/pingcap/*`
  4. `github.com/tikv/pd/*`
  5. Blank imports last

- **Error handling**: Use `github.com/pingcap/errors` for wrapping (`errors.Wrap`, `errors.Annotate`) or `fmt.Errorf` with `%w`
- **Error strings**: lowercase, no trailing punctuation
- **Context**: First parameter for functions with external effects
- **Constants**: Use typed constants with `iota` for enums
- **Naming**: 
  - Acronyms uppercase (TSO, API, HTTP, not Tso, Api, Http)
  - Package names lowercase, no underscores
  - Avoid stuttering (prefer `pd.Server` over `pd.PDServer`)

### File Headers
All Go files must include the Apache 2.0 license header:
```go
// Copyright 2024 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
```
Note: Use the current year when creating new files.

### Prohibited Dependencies
The following packages are banned by depguard and must NOT be used:
- `github.com/pkg/errors` - Use `github.com/pingcap/errors` instead
- `go.uber.org/atomic` - Use standard library `sync/atomic` instead
- `math/rand` - Use `math/rand/v2` for new code

### Concurrency
- Always use pointer for `sync.WaitGroup` (enforced by revive)
- Prevent goroutine leaks with proper cancellation
- Use `context.Context` for cancellation, never store in structs
- Close channels, timers, and tickers with defer

### Logging and Metrics
- Use structured logging (zap) with meaningful fields
- Never log secrets or PII
- Use Prometheus-style metrics with appropriate labels
- Avoid high-cardinality label values

## Testing

### Failpoints
Failpoints are used for fault injection in tests. Important rules:
- Failpoints should only be enabled during tests
- Use make targets that auto-enable/disable failpoints
- Never commit generated failpoint files
- Run `make failpoint-disable` or `make clean-test` after testing

### Test Types
- **Unit tests**: `make basic-test` (fast, no race detection)
- **Full tests**: `make test` (with race detection and coverage)
- **TSO tests**: `make test-tso-function`
- **Real cluster tests**: `make test-real-cluster`
- **CI tests**: `make ci-test-job`

### Test Patterns
- Mirror patterns from existing tests
- Use testify assertions where appropriate
- Follow table-driven test patterns
- Add race detection for concurrent code: `go test -race`

## API and Documentation

### Swagger
- Regenerate with `SWAGGER=1 make build` or `make swagger-spec`
- Keep annotations current with API changes
- API handlers should use `errcode` for error responses

### JSON
- Use explicit JSON tags
- Apply `omitempty` for optional fields
- Consider easyjson for performance-critical paths

## Pull Request Guidelines

### Commit Messages
Format: `pkg: description` (≤70 chars)

Example:
```
server: fix TSO allocation race condition

This fixes a race condition in TSO allocation that could cause
duplicate timestamps under high load.

* Add mutex to protect allocation counter
* Add test to verify race-free behavior
```

### PR Requirements
- Include `Issue Number: close #123` or `ref #456` in PR description
- Sign off commits with `git commit -s` (DCO requirement)
- Ensure all CI checks pass
- Get approval from two reviewers (LGTM x2)

## Special Considerations

### Dashboard
- PD includes an embedded web dashboard (requires CGO)
- Disable for faster builds: `DASHBOARD=0 make build` or `make pd-server-basic`
- Dashboard assets are embedded via `scripts/embed-dashboard-ui.sh`

### Client Submodule
The `client/` directory is a separate Go module:
- Has its own Makefile and dependencies
- Run tests from within `client/`: `cd client && make test`
- Keep synchronized with main module

### Microservices Mode
- `NEXT_GEN=1` builds use `nextgen` tag
- Resource group and keyspace features follow specific patterns
- Maintain compatibility with both modes

## Security

- Never commit secrets, keys, or tokens
- Use `crypto/rand` for security-critical randomness
- Validate all HTTP/gRPC inputs
- Return appropriate error codes, never panic in handlers
- Be cautious with error messages (avoid leaking sensitive info)

## Additional Resources

- [AGENTS.md](../AGENTS.md) - Comprehensive agent and contributor guide
- [CONTRIBUTING.md](../CONTRIBUTING.md) - Contribution workflow and guidelines
- [README.md](../README.md) - Project overview and setup
- [PD Documentation](https://docs.pingcap.com/tidb/stable/pd-configuration-file)

## Common Tasks

### Adding a New API Endpoint
1. Define handler in appropriate server package
2. Add route registration
3. Update Swagger annotations if `SWAGGER=1`
4. Add integration test
5. Update documentation

### Adding a New Metric
1. Define metric in `metrics/` package
2. Register in appropriate subsystem
3. Export and use in code
4. Add test if feasible

### Modifying Schedulers
1. Check existing scheduler patterns
2. Update scheduler interfaces if needed
3. Add comprehensive tests with failpoints
4. Consider impact on existing clusters

### Working with etcd
1. Use existing etcd client wrappers
2. Handle errors and retries properly
3. Consider transaction boundaries
4. Test with failpoints for network failures

---

**Remember**: Always consult [AGENTS.md](../AGENTS.md) for detailed development guidelines and [CONTRIBUTING.md](../CONTRIBUTING.md) for the contribution process.
