# PD Agent Guide

- Scope: applies to the whole repository unless a deeper `AGENTS.md` overrides it.
- Repo shape: root Go module plus the `client/` submodule.
- Go baseline: keep the local toolchain aligned with the version declared in the relevant `go.mod`.
- Main binaries: `pd-server`, `pd-ctl`, `pd-recover`.

## Start Here
- Prefer repo `make` targets over ad-hoc commands; they wire tags, failpoints, generators, and dashboard assets correctly.
- Check for a deeper `AGENTS.md` before editing a subtree.

## Common Entry Points
- Root build: `make build`
- Root full loop: `make dev`
- Root lightweight loop: `make dev-basic`
- Root validation: `make check`
- Root fast tests: `make basic-test`
- Root targeted tests: `make gotest GOTEST_ARGS='./pkg/foo -run TestBar -count=1'`
- `client/` default pipeline: run `make` inside `client/`
- `client/` fast tests: `make basic-test`
- `client/` full tests: `make test`

## Failpoints Discipline
- PD uses `github.com/pingcap/failpoint` to inject test-only branches, returns, and pauses into specific code paths.
- Keep failpoints enabled only for tests; disable immediately after (`make failpoint-disable` or `make clean-test`) to avoid polluting the codebase.
- Prefer make targets that auto-enable/disable failpoints (recommended: `make gotest ...`, `make test`, `make basic-test`).
- If you must run `go test` manually, use this rule:
  - Target uses failpoints (for example imports `github.com/pingcap/failpoint`): `make failpoint-enable` -> `go test ...` -> `make failpoint-disable`.
  - Target does not use failpoints: run `go test ...` directly.
- Runtime model:
  - `failpoint.Enable` / `failpoint.Disable` only change runtime evaluation state.
  - A failpoint takes effect only when execution reaches the injected site again.
  - Enabling a failpoint does not replay startup or initialization logic that has already finished.
- If failpoint semantics are unclear, inspect the injected code path and nearby tests before changing test flow or assertions.
- Useful lookup pattern: `rg -n "failpoint.Inject|failpoint.InjectCall|failpoint.Enable" pkg tests server`.
- If that is still not enough, read the upstream `README.md` in `github.com/pingcap/failpoint` before changing the test design.
- When tests need controllable fault injection, compare `failpoint` and `mock` first; prefer the simpler and more maintainable option instead of defaulting to `mock`.
- Never edit code or run non-test commands while failpoints are enabled. If unsure about state, run `make failpoint-disable` before continuing.
- Never commit generated failpoint files or leave failpoints enabled; verify `git status` is clean before pushing.
- If failpoint-related tests misbehave, rerun after `make failpoint-disable && make failpoint-enable` to ensure a clean state.

## Generated Or Derived Files
- `make check` includes `make generate-errdoc`; error-code changes may require updating `errors.toml`.
- Swagger output is not part of a normal build unless `SWAGGER=1`; use `SWAGGER=1 make build` or `make swagger-spec` when API annotations change.
- Easyjson generation is explicit: `make generate-easyjson` updates `pkg/response/region.go`.
- Dashboard assets are embedded; use `make dashboard-ui` when touching UI assets.
- For faster local server builds, skip dashboard with `DASHBOARD=0` or `make pd-server-basic`.
- Custom dashboard distributions use `DASHBOARD_DISTRIBUTION_DIR`; distro info replacement uses `make dashboard-replace-distro-info`.
- Pinned tooling is installed into `.tools/bin` via `make install-tools`.
- `make tidy` must leave `go.mod` and `go.sum` clean.

## Lint And CI Traps
- `.golangci.yml` is intentionally strict; check it before fighting the linter.
- Non-obvious depguard bans: `github.com/pkg/errors`, `go.uber.org/atomic`, and `math/rand` in favor of `math/rand/v2`.
- Import order is enforced as: stdlib, third-party, `github.com/pingcap`, `github.com/tikv/pd`, blank.
- `make static` and `make check` respect repo-specific package selectors and validations; prefer them to handcrafted lint commands.

## Special Test Modes
- `make test` is the full deadlock/race/cover path.
- `make test-tso-function` uses the `without_dashboard,deadlock` tags.
- `make ci-test-job JOB_INDEX=N` mirrors CI sharding.
- `make ut` runs the `pd-ut` harness instead of plain `go test`.
- `make test-real-cluster` uses `tests/integrations/realcluster` and wipes `~/.tiup/data/pd_real_cluster_test`.
- `NEXT_GEN=1` enables the `nextgen` tag and disables dashboard-related paths.

## Contribution Conventions
- PRs follow `.github/pull_request_template.md` and should include `Issue Number: close|ref #...`.
- Commit subject format is `pkg: message`, max 70 chars; use `*:` for broad changes.
- Use `git commit -s`.
- Before opening a PR, run `make check` and the narrowest relevant test target. In most cases, that means at least `make basic-test` or `make gotest ...`.

## Repo-Specific Skills
- Reusable agent workflows live in `.agents/skills/`.
- [`add-metrics`](.agents/skills/add-metrics/SKILL.md): adds or changes PD
  Prometheus instrumentation. Inputs: target package or file plus metric name,
  type, labels, and rollout context. Outputs: metric patch and migration notes.
  Constraints: cache `WithLabelValues`, avoid hot-path instrumentation, control
  cardinality, and keep existing metrics backward compatible.
- [`fix-cherry-pick-pr`](.agents/skills/fix-cherry-pick-pr/SKILL.md): repairs
  release-branch cherry-pick PRs. Inputs: source PR, cherry-pick PR or branch,
  and touched files or tests. Outputs: repaired branch plus parity and
  verification report. Constraints: preserve release-branch-only behavior,
  compare final aggregate diffs, and follow PD failpoint discipline.
- [`create-pr`](.agents/skills/create-pr/SKILL.md): pushes the current branch
  and opens a PR that matches the PD template. Inputs: current branch,
  committed diff, issue refs, and template context. Outputs: reviewed title and
  body draft, pushed branch, and PR URL. Constraints: show the draft before
  submission, use `gh`, and never force-push without approval.
