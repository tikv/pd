# PD Go Best Practices Checklist

Use this reference as a repository-specific checklist for `pd` implementation and review work. Focus on conventions that are easy to miss if you rely only on generic Go style guidance or on `make check`.

## Repository rules come first

- Follow the root `AGENTS.md`, deeper `AGENTS.md` files, and `.github/copilot-instructions.md` before applying generic Go advice.
- Treat existing code in the touched package as the primary style baseline unless the task explicitly asks for a cleanup or redesign.
- Do not restate issues already enforced by `make check` unless they connect to a broader PD-specific concern.

## Errors and HTTP handling

- Keep error handling consistent with existing `pd` code: prefer `github.com/pingcap/errors` wrapping/annotation patterns or `fmt.Errorf(... %w ...)` where that is already the local style.
- Do not introduce mixed error-handling conventions in the same code path.
- For HTTP handlers, follow the repository's `errcode` and `errorResp` patterns instead of using `http.Error` directly.
- If a change affects structured error definitions, keep `errors.toml` and generated error docs in sync.

## Tests and failpoints

- Prefer repository make targets such as `make gotest`, `make basic-test`, or `make test` so failpoints are enabled and disabled correctly.
- Do not leave failpoints enabled after testing, and do not run non-test commands while failpoints are enabled.
- Preserve failpoint-aware test flows when changing code paths that already use failpoints.
- When behavior is covered by integration-style tests, update those tests with the code change instead of narrowing coverage to mocks only.

## Naming, packages, and API fit

- Match existing `pd` package boundaries and naming patterns before introducing new helpers or packages.
- Keep exported API changes narrow; do not rename exported identifiers or move packages broadly unless the task explicitly requires that scope.
- Prefer extending existing modules over adding generic helper packages that do not fit a clear PD domain.

## Concurrency, cleanup, and runtime behavior

- Pair goroutines, timers, and tickers with explicit cancellation or cleanup.
- Keep lock usage consistent with surrounding code and preserve existing synchronization assumptions.
- Close resources on every exit path and preserve existing cleanup behavior in long-running services.
- Avoid new mutable package-level state unless the surrounding code already uses that pattern and the task requires it.

## PD-specific review heuristics

- Prefer small, behavior-preserving edits that fit the current package and call flow.
- When local repository convention conflicts with generic Go guidance, keep the `pd` pattern unless there is a clear task-driven reason to change it.
- In reviews, call out repository-specific issues directly: incompatible error style, broken failpoint flow, incorrect HTTP error handling, compatibility-risky API churn, or changes that drift from established package boundaries.
