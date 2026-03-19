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

## Failpoints
- Many PD test paths rely on failpoints. Safest path is to use `make gotest`, `make test`, or `make basic-test`, which manage them for you.
- If a package under test imports `github.com/pingcap/failpoint` and you run raw `go test`, bracket it with `make failpoint-enable` and `make failpoint-disable`.
- Do not edit code, build binaries, or commit while failpoints are enabled.
- If failpoint state is unclear or behavior looks polluted, reset with `make failpoint-disable` or `make clean-test` before continuing.
- Never commit generated failpoint artifacts.

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
- `fix-cherry-pick-pr`: repairs PD cherry-pick PRs while preserving release-branch-only behavior and validating failpoint-sensitive diffs.
- `create-pr`: pushes the current branch and opens a PR that matches the PD template.
