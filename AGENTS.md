# PD Agent Guide

- Scope: Governs the entire repository. If another `AGENTS.md` exists deeper, it overrides in its subtree.
- Cursor rules: none in `.cursor/rules/` or `.cursorrules`.
- Copilot rules: see `.github/copilot-instructions.md`.
- Purpose: Give human and agent contributors a concise, reliable playbook.

## Quick Facts
- Language: Go modules (root + `client/` submodule).
- Go version: CI uses 1.25 (install >=1.25).
- Main binaries: `pd-server`, `pd-ctl`, `pd-recover`, tool suite.

## Build Shortcuts
- Default all: `make build` (pd-server, pd-ctl, pd-recover).
- Full dev loop: `make dev` (build + check + tools + test).
- Lightweight: `make dev-basic` (build + check + basic-test).
- No dashboard: `make pd-server-basic` (sets `SWAGGER=0 DASHBOARD=0`).
- With swagger spec: `SWAGGER=1 make build` (runs `swagger-spec`).
- With custom dashboard distro: set `DASHBOARD_DISTRIBUTION_DIR` then `make build`.
- Specific tool: `make pd-ctl`, `make pd-tso-bench`, etc.
- Race build: `WITH_RACE=1 make build` (CGO on, `-race`).
- FIPS/boringcrypto: `ENABLE_FIPS=1 make build`.
- Simulator: `make simulator`.
- Docker image: `make docker-image` (needs Docker daemon).

## Client Module (client/)
- Default pipeline: from `client/`, run `make` (static + tidy + test).
- Tests with race/tags: `make test` (deadlock tag, race, cover; auto failpoints).
- Fast tests: `make basic-test`.
- CI coverage: `make ci-test-job`.
- Lint/static: `make static` (gofmt, golangci-lint, leakcheck).

## Lint & Static Analysis
- Primary entry: `make check` (tidy + static + generate-errdoc).
- Static combo: `make static` runs gofmt -s, golangci-lint, leakcheck (PACKAGE_DIRECTORIES/SUBMODULES respected).
- Tidy: `make tidy` must leave `go.mod`/`go.sum` clean (CI enforces empty diff).
- Error docs: `make generate-errdoc` (updates `errors.toml`).
- golangci-lint config: see `.golangci.yml` (gofmt/goimports/gci formatters; depguard bans `github.com/pkg/errors`, `go.uber.org/atomic`, `math/rand`; prefers `math/rand/v2`; goheader copyright; revive + testifylint extensive; waitgroup-by-value forbidden).
- leakcheck excludes `tests/server/join/join_test.go`.
- Formatter order (gci): standard, default, `prefix(github.com/pingcap)`, `prefix(github.com/tikv/pd)`, blank.

## Test Matrix
- Full suite: `make test` (tags deadlock, CGO=1, race, cover; auto failpoints).
- Basic fast: `make basic-test` (no tests/ packages, no race; failpoints enabled).
- Targeted (single pkg/test): `make gotest GOTEST_ARGS='./pkg/foo -run TestBar -count=1'` (auto failpoints).
- UT binary: `make ut` -> `./bin/pd-ut run --ignore tests --race --junitfile ./junitfile`.
- CI shard: `make ci-test-job JOB_INDEX=N` (needs dashboard-ui + pd-ut built).
- TSO function: `make test-tso-function` (tags `without_dashboard,deadlock`, race on).
- Real cluster: `make test-real-cluster` (uses `tests/integrations/realcluster`; wipes `~/.tiup/data/pd_real_cluster_test`).
- Coverage split: `make test-with-cover-parallel` after `make split`.
- Clean test artifacts: `make clean-test` (clears `/tmp/pd_tests*`, go test cache, UT bins, playground log).

## Failpoints Discipline
- Keep failpoints enabled only for tests; disable immediately after (`make failpoint-disable` or `make clean-test`) to avoid polluting the codebase.
- Prefer make targets that auto-enable/disable failpoints (recommended: `make gotest ...`, `make test`, `make basic-test`).
- If you must run `go test` manually, use this rule:
  - Target uses failpoints (for example imports `github.com/pingcap/failpoint`): `make failpoint-enable` -> `go test ...` -> `make failpoint-disable`.
  - Target does not use failpoints: run `go test ...` directly.
- Never edit code or run non-test commands while failpoints are enabled. If unsure about state, run `make failpoint-disable` before continuing.
- Never commit generated failpoint files or leave failpoints enabled; verify `git status` is clean before pushing.
- If failpoint-related tests misbehave, rerun after `make failpoint-disable && make failpoint-enable` to ensure a clean state.

## Imports & Formatting
- Use gci/goimports ordering: stdlib | third-party | pingcap | tikv/pd | blank.
- Avoid dot-imports; revive rule will warn.
- Keep imports deduped; run gofmt/goimports/gci.
- gofmt rewrite rule: `interface{}` -> `any` (see `make fmt`).
- Run `make fmt` or gofmt on touched files; respect project ordering.

## Code Style Essentials
- Prefer concrete types; keep structs zero-value friendly; init maps/slices before use.
- Pointers for large structs; avoid pointer-to-interface and copying mutex holders.
- Typed constants with iota for enums; keep error codes in `errors.toml` (regenerate via `make generate-errdoc`).
- First param `context.Context` for external effects; never store contexts in structs.
- Acronyms uppercase (TSO, API, HTTP); package dirs lower_snake; filenames kebab/underscore ok.
- Exported identifiers need GoDoc starting with the name; avoid stutter (`pd.PDServer` -> `Server`).

## Error Handling
- Wrap with `github.com/pingcap/errors` (`errors.Wrap`/`Annotate`) or `fmt.Errorf("...: %w", err)`.
- No ignored errors unless allowed in `.golangci.yml` errcheck exclusions.
- Error strings: lowercase, no trailing punctuation.
- Prefer sentinel errors over panic; panic only on programmer bugs/impossible states.
- HTTP handlers: use errcode + `errorResp`; avoid `http.Error`.

## Logging
- Use existing structured logging (zap/log); avoid `fmt.Println`.
- Include useful fields (component, store id, region id); never log secrets/PII.

## Concurrency
- Cancel timers/tickers; close resources with defer and error checks.
- `sync.WaitGroup` must be a pointer (revive rule).
- Prevent goroutine leaks: pair with cancellation; consider errgroup.
- Guard shared state with mutex/RWMutex; keep lock ordering consistent.

## Collections & Ranges
- Do not capture loop vars by pointer; copy inside loop (`v := val`).
- Range-in-closure/address rules enforced; make local copies.
- Preallocate slices/maps when size known.

## API / JSON / Docs
- Swagger: `make swagger-spec` (SWAGGER=1) regenerates; keep annotations current.
- Easyjson: `make generate-easyjson` updates `pkg/response/region.go`.
- JSON tags explicit; use `omitempty` where sensible.
- Update docs when API/build/test flows change.

## Metrics & Telemetry
- Prometheus-style metrics; name with subsystem + unit; avoid high-cardinality labels.
- `metrics/` package has helpers; add tests for new metrics when feasible.

## Performance
- Mind allocations; reuse buffers/pools where appropriate.
- Avoid per-request reflection; keep hot paths lean.
- Use context-aware timeouts and backoff for retries.

## Security
- Never commit secrets/keys/tokens.
- Use `crypto/rand` for security needs; avoid insecure randomness.
- Do not disable lint/security checks without discussion.

## Dependencies & Tools
- Tools live in `.tools/bin`; `make install-tools` installs pinned versions (golangci-lint v2.6.0, etc.).
- `tools.go` pins tool deps; avoid adding runtime deps without justification; run `make tidy` after changes and ensure clean diff.

## Dashboard / UI
- Assets embedded via `scripts/embed-dashboard-ui.sh`; run through `make dashboard-ui` dependency.
- Skip dashboard for speed with `DASHBOARD=0` or `make pd-server-basic`.
- Custom distro info: set `DASHBOARD_DISTRIBUTION_DIR` then `make dashboard-replace-distro-info`.

## Repository Hygiene
- Run `make tidy` after dep changes; `go.mod`/`go.sum` must stay clean.
- Run `make fmt`/gofmt; ensure gci/goimports ordering.
- Clean with `make clean` (removes failpoints, tmp tests, bin, `.tools/bin`).
- Avoid committing generated artifacts (junitfile, coverage, dashboard caches) unless required.

## PRs & Commits
- Follow `.github/pull_request_template.md`; include `Issue Number: close|ref #...` line.
- Commit subject: `pkg: message`, <=70 chars; blank line; body wrapped 80 chars; add `Signed-off-by` via `git commit -s`.
- Multi-area: separate packages with commas; use `*:` for broad changes.

## Microservices / NextGen
- `NEXT_GEN=1` builds/tests use `nextgen` tag and disable dashboard; ensure compatibility.
- Resource group/keyspace features live under `server/` resource dirs—follow existing patterns.

## Failure Handling
- Return errors up-stack; use `errors.Is/As` for sentinel checks.
- Ensure cleanups on all exits; prefer named cleanups with defer.

## Interacting with External Systems
- gRPC services use interceptors; add auth/validation in handlers, not transport.
- HTTP handlers validate payloads and return proper status codes; avoid panics.

## Missing Context?
- Mirror patterns in existing tests for expected behavior.
- Use `git blame`/`git log` to follow conventions; avoid history rewrites.

## Final Checklist
- Read this file; look for deeper `AGENTS.md` before editing.
- Before PR: run `make check` and the narrowest relevant `go test` (with tags). At minimum run `make basic-test` for touched packages.
- Keep imports ordered, gofmt clean, modules tidy.
- Respect depguard bans, revive/testifylint findings; fix before submission.
- Ensure `PD_EDITION` set correctly; enable failpoints where required.
- Keep error handling consistent with `pingcap/errors` and errcode usage.
- Avoid new global state; keep concurrency safe; close resources.
- Leave workspace clean (no stray tmp, coverage, or generated dashboard files).
