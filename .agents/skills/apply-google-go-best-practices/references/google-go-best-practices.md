# Google Go Best Practices Checklist

Paraphrased from the official Google guide:
https://google.github.io/styleguide/go/best-practices

Use this reference as a checklist for review and implementation work. Follow repository-local rules first when they are stricter or when changing a public API would be too disruptive.

## Naming and package layout

- Omit repeated package, receiver, parameter, result, or pointer information from names when the call site already makes it obvious.
- Use noun-like names for accessors and values; avoid `Get` unless it disambiguates an unusual case.
- Use verb-like names for operations with effects.
- If two helpers differ mainly by type, suffix the type where it clarifies the distinction.
- Avoid overly generic `util`, `common`, or `helpers` packages. Prefer helpers near the domain they support or in a narrowly scoped `internal` package.
- Choose package boundaries based on caller ergonomics and implementation coupling, not file count. Keep tightly coupled types together when users typically need them together.

## Tests, doubles, and shadowing

- Name test doubles by role or behavior when that improves clarity, for example `fake`, `stub`, `spy`, or `recorder`.
- Reusable doubles for one production package can live in a dedicated `foo_test`-style helper package.
- In tests, choose local names that clearly distinguish doubles from production types when both appear together.
- Avoid `:=` that accidentally shadows outer `err`, `ctx`, `cancel`, loop variables, or state used after the block.
- Prefer plain assignment or a new explicit name when narrowing scope would otherwise hide important state.

## Error handling and panics

- Return structured errors when callers need to branch on the failure. Do not force callers to inspect error strings.
- Add context only when it contributes new information, such as the failed operation or boundary crossing.
- Use `%w` when callers should still be able to inspect the wrapped cause with `errors.Is` or `errors.As`.
- Log errors only when the function is intentionally swallowing them or when crossing a system boundary. Avoid duplicate log-and-return behavior.
- Propagate program startup and configuration errors to `main`, then emit an actionable message there.
- Prefer ordinary error returns over panics. Reserve panic for impossible states, programmer bugs, or tightly contained internal control flow with a matching recover.

## Documentation

- Keep Godoc conventional and preview rendered docs when changing exported APIs or comments.
- Document parameter and configuration behavior only when it is non-obvious or surprising.
- Document context lifetime, cancellation, or alternate interruption rules when they are not obvious from the signature.
- Document concurrency guarantees for mutating operations and any unusual read-safety behavior.
- Document cleanup ownership clearly: what must be closed, stopped, canceled, or freed, and by whom.
- Document important sentinel errors or error types so callers know what conditions they can handle explicitly.
- Add short signal-boosting comments for easy-to-misread conditions or intentionally unusual code paths.

## Variables and API shape

- Prefer zero-value declarations for empty values that are ready for later use.
- Use composite literals when initial fields or elements matter to the reader.
- Preallocate slices or maps only when the target size is known or measurement shows it matters.
- Specify channel direction when the API can express send-only or receive-only ownership.
- Use an option struct when an argument list is long, unstable, or contains many optional fields.
- Use variadic option functions when call sites benefit from zero-cost defaults and composable configuration values.
- Keep complex CLI surfaces structured; do not let flags and config handling sprawl into ambiguous ad hoc APIs.

## Testing

- Keep the main validation logic in the `Test...` function. Helpers may fail fast when setup fails and the test cannot proceed.
- Prefer using real clients or transports connected to test servers over hand-written stand-ins for complicated protocols.
- Use `t.Error` for failures that should not stop the rest of the test.
- Use `t.Fatal` only when the current test or subtest cannot continue safely.
- In table-driven tests without subtests, pair `t.Error` with `continue` for per-case failures.
- In `t.Run` subtests, `t.Fatal` may stop only the current subtest.
- Never call `t.Fatal`, `t.FailNow`, or similar APIs from spawned goroutines.
- Keep setup close to the tests that need it. Use `TestMain` only for package-wide expensive setup that truly requires teardown.
- Amortize expensive setup only when it is shared, safe, and does not need teardown.

## Strings and constants

- Use `+` for a few simple string fragments.
- Use `fmt.Sprintf` or `fmt.Fprintf` when formatting is the point.
- Use `strings.Builder` when constructing a string incrementally across many steps.
- Prefer raw string literals for constant multiline strings.

## Global state

- Avoid mutable package-level state in reusable libraries.
- Treat package state and global service locators as risky when otherwise independent callers or tests can interfere with each other.
- If a convenience package-level API is necessary, also provide instance-based APIs so callers can isolate dependencies and tests.
- Keep package-level convenience APIs for binaries or thin entrypoints, not for broadly imported infrastructure libraries.

## Apply in pd

- Follow `/Users/ryan/Workspace/pd/AGENTS.md`, deeper `AGENTS.md` files, `.github/copilot-instructions.md`, and `.golangci.yml` before applying generic style guidance.
- Preserve PD-specific conventions such as `github.com/pingcap/errors`, failpoint-safe test flows, and gci import ordering.
- Prefer small, behavior-preserving edits. Do not rename exported APIs or move packages broadly unless the task explicitly requires that scope.
