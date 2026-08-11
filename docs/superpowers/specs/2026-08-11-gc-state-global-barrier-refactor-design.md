# GC state global barrier refactor design

This design refactors the `pd-ctl gc-state` command introduced by PR #11054.
The refactor uses the enhanced `GetGCState` API from
[PR #11117](https://github.com/tikv/pd/pull/11117) to include global GC
barriers in single-keyspace diagnostics, removes the standalone `global`
subcommand, and adds an option that omits global barriers from either remaining
view.

## Context

PR #11054 currently exposes three views: `keyspace`, `global`, and `all`. The
`global` view calls `GetAllKeyspacesGCStates`, even though it discards every
keyspace state and only emits global barriers. That call still enumerates and
materializes all keyspace states, so it performs unnecessary work on clusters
with many keyspaces.

PR #11117 adds an opt-in global barrier result to `GetGCState`. The client uses
`gc.ExcludeGlobalGCBarriers(false)` to request the result and preserves whether
the server omitted the result, returned an empty list, or returned a populated
list. The server reads the selected keyspace state and global barriers in one
revision-validated operation.

## Goals and non-goals

The refactor keeps the CLI focused on the two diagnostic scopes that have
distinct data requirements.

The design has these goals:

- Make `gc-state keyspace` return the selected effective GC state, local
  barriers, and global barriers with one `GetGCState` call.
- Keep `gc-state all` as the only command that enumerates every effective GC
  scope.
- Remove `gc-state global` and every code, test, help, and documentation path
  that exists only for that subcommand.
- Let users skip global barrier reads and output in both remaining subcommands
  with `--exclude-global-barriers`.
- Preserve the difference between an omitted global barrier result and a
  requested result that contains an empty list.
- Preserve deterministic sorting, expired barrier filtering, effective scope
  handling, and actionable compatibility errors.

The design does not add another PD RPC, automatically retry failed requests,
change GC state semantics, or change how PD stores and expires barriers.

## CLI contract

The command tree contains only the two views that correspond to a selected
keyspace or all effective scopes:

```text
gc-state
├── keyspace <keyspace-id>
└── all
```

The implementation removes `gc-state global` without a deprecated or hidden
alias. PR #11054 has not shipped, so the command does not have a released
compatibility contract.

### Shared flags

The parent `gc-state` command defines two persistent flags that both subcommands
inherit:

- `--include-expired` includes zero-TTL local and requested global barriers.
- `--exclude-global-barriers` skips the global barrier read and omits the
  `global_gc_barriers` JSON field.

Both flags default to `false`. Explicitly setting
`--exclude-global-barriers=false` produces the default complete view.

### Behavior matrix

The following matrix defines the request and output behavior for every flag
combination.

| Command | Local barriers | Global barriers | Global JSON field |
| --- | --- | --- | --- |
| `keyspace 42` | Read; hide expired | Read; hide expired | Present |
| `keyspace 42 --include-expired` | Read; include expired | Read; include expired | Present |
| `keyspace 42 --exclude-global-barriers` | Read; hide expired | Do not read | Omitted |
| `keyspace 42 --exclude-global-barriers --include-expired` | Read; include expired | Do not read | Omitted |
| `all` | Read; hide expired | Read; hide expired | Present |
| `all --include-expired` | Read; include expired | Read; include expired | Present |
| `all --exclude-global-barriers` | Read; hide expired | Do not read | Omitted |
| `all --exclude-global-barriers --include-expired` | Read; include expired | Do not read | Omitted |

When global barriers are included, the JSON field is present even if the list
is empty. Therefore, `"global_gc_barriers": []` means the command requested
global barriers and PD returned none. A missing field means the user explicitly
excluded them.

## Request flow

Each subcommand uses the least expensive RPC that provides all data required by
that view.

### Keyspace view

The keyspace view calls `GetGCState` exactly once. Its default client options
are:

```go
GetGCState(
	ctx,
	gc.ExcludeGCBarriers(false),
	gc.ExcludeGlobalGCBarriers(false),
)
```

The returned `gc.GCState` contains the effective keyspace state, local
barriers, and global barriers from the same server-side read. With
`--exclude-global-barriers`, the command changes only the second option to
`gc.ExcludeGlobalGCBarriers(true)`.

The command never calls `GetAllKeyspacesGCStates` as a fallback for the
keyspace view.

### All view

The all view continues to call `GetAllKeyspacesGCStates` because it must
enumerate every effective GC scope. It always requests local barriers and maps
the flag directly to `gc.ExcludeGlobalGCBarriers`.

The all view does not make an additional `GetGCState` call. No remaining path
calls `GetAllKeyspacesGCStates` solely to obtain global barriers.

## Internal command structure

The reader abstraction describes the two command behaviors and carries one
positive boolean that controls global barrier inclusion:

```go
type gcStateReader interface {
	getGCState(
		ctx context.Context,
		keyspaceID uint32,
		includeGlobalGCBarriers bool,
	) (gc.GCState, error)
	getAllKeyspacesGCStates(
		ctx context.Context,
		includeGlobalGCBarriers bool,
	) (gc.ClusterGCStates, error)
	close()
}
```

The boolean is named `includeGlobalGCBarriers` at every declaration and call
site. The Cobra layer converts the negative flag once:

```go
includeGlobalGCBarriers := !excludeGlobalGCBarriers
```

The concrete reader maps the positive value to the client option with
`gc.ExcludeGlobalGCBarriers(!includeGlobalGCBarriers)`. A shared pure helper
returns the local and global barrier options for both RPCs. This helper keeps
option construction consistent and lets unit tests verify the mapping without
mocking the full PD client.

The implementation deletes these obsolete elements:

- `getGlobalGCState` from the reader interface and concrete reader.
- `getGlobalCalls` from the fake reader.
- `clusterGCStatesClient` and `readClusterGCStates`.
- `newGCStateGlobalCommand` and its command registration.
- `globalGCStateOutput` and `newGlobalGCStateOutput`.

## JSON projection

The command output must represent the client's three global barrier states:
not requested, requested and empty, and requested and populated.

A slice with `omitempty` cannot express that contract because JSON encoding
omits both nil and empty slices. The keyspace and all output structs use a
pointer to a slice instead:

```go
type keyspaceGCStateOutput struct {
	RequestedKeyspaceID uint32             `json:"requested_keyspace_id"`
	EffectiveKeyspaceID uint32             `json:"effective_keyspace_id"`
	IsKeyspaceLevelGC   bool               `json:"is_keyspace_level_gc"`
	TxnSafePoint        uint64             `json:"txn_safe_point"`
	GCSafePoint         uint64             `json:"gc_safe_point"`
	GCBarriers          []gcBarrierOutput  `json:"gc_barriers"`
	GlobalGCBarriers    *[]gcBarrierOutput `json:"global_gc_barriers,omitempty"`
}

type allGCStatesOutput struct {
	GCStates         []gcStateOutput    `json:"gc_states"`
	GlobalGCBarriers *[]gcBarrierOutput `json:"global_gc_barriers,omitempty"`
}
```

When global barriers are excluded, the pointer remains nil, the converter does
not call `GetGlobalGCBarriers`, and JSON encoding omits the field. When global
barriers are included, the pointer references a non-nil slice. The field then
encodes as either `[]` or a populated array.

`newKeyspaceGCStateOutput` and `newAllGCStatesOutput` accept
`includeGlobalGCBarriers` in addition to `includeExpired`. They obtain global
barriers only when requested and reuse `newGlobalGCBarrierOutputs` for nil
entry filtering, expired entry filtering, TTL conversion, and deterministic
sorting.

Local barrier fields remain required and encode empty results as
`"gc_barriers": []`.

## Compatibility and errors

The command reports capability mismatches explicitly and does not convert an
absent response wrapper into an empty barrier list.

During a rolling upgrade, an older PD server can ignore the new opt-in request
field and return a successful `GetGCState` response without the global barrier
wrapper. The client then reports `state.HasGlobalGCBarriers() == false`. If the
default keyspace view requires global barriers, the command returns this
actionable error:

```text
gc-state keyspace requires a PD server whose GetGCState supports global GC barriers; retry with --exclude-global-barriers
```

The explicit exclusion flag remains a valid degraded path for reading safe
points and local barriers from that server.

If the all view requires global barriers but the cluster result does not carry
them, the command returns:

```text
gc-state all response does not include global GC barriers; retry with --exclude-global-barriers
```

The command does not retry automatically. A retry could perform another
server-side read, mix snapshots, hide a rolling-upgrade capability difference,
and reintroduce unnecessary work.

Existing error behavior remains intact for an unimplemented RPC, client
creation failure, other RPC errors, missing local barriers, JSON encoding, and
output writes. Every path closes the reader after successful creation.

## Rebase integration

The implementation starts by rebasing PR #11054 onto `upstream/master`, which
already contains PR #11117. Both changes modify the public GC state model and
protobuf conversion, so conflict resolution must combine their behavior.

The resolved client model retains all of these elements:

- `IsKeyspaceLevelGC` from PR #11054.
- `hasGlobalGCBarriers` and `globalGCBarriers` from PR #11117.
- `WithGlobalGCBarriers`, `HasGlobalGCBarriers`, and
  `GetGlobalGCBarriers` from PR #11117.

`pbToGCState` constructs the local state and sets `IsKeyspaceLevelGC`.
`pbToGCStateWithGlobalGCBarriers` then attaches the optional global barrier
result without losing the keyspace-level flag. The client tests cover this
composition without duplicating PR #11117's storage, snapshot, TTL, and rolling
upgrade test coverage.

## Test design

The test suite proves the option mapping, command routing, JSON presence
contract, compatibility behavior, and real PD integration in Classic and
NextGen configurations.

### Unit tests

The command unit tests cover these cases:

- Both reader methods always request local barriers.
- Both reader methods include global barriers by default and exclude them only
  when requested.
- `keyspace` calls only `getGCState`, and `all` calls only
  `getAllKeyspacesGCStates`.
- The fake reader receives the expected `includeGlobalGCBarriers` value.
- `global` is an unknown subcommand and fails before creating a reader.
- Root and subcommand help mention only `keyspace`, `all`, and the two shared
  flags.
- Default projections emit an empty or populated global array.
- Excluded projections omit the global field and do not require the client
  model to carry global barriers.
- Required but absent global barriers produce the actionable compatibility
  errors.
- Local and global nil entries are skipped.
- Local and global barriers use the same active and expired filtering rules.
- Barrier arrays preserve deterministic sorting and TTL conversion.
- Effective scope filtering still omits unified-GC placeholders and retains
  the NullKeyspace state.
- Reader closure and output error propagation remain intact.

Tests inspect encoded JSON maps in addition to Go values so they detect field
presence regressions caused by `omitempty`.

### Integration tests

The existing safepoint test fixtures already contain keyspace-level,
NullKeyspace, unified-GC, active, expired, local, and global barrier cases. The
refactor reuses those fixtures and changes the command assertions.

The integration tests verify these behaviors:

- Default keyspace output includes the same global barriers for keyspace-level,
  NullKeyspace, and unified-GC requests.
- `keyspace --include-expired` includes zero-TTL local and global barriers.
- `keyspace --exclude-global-barriers` preserves safe points and local barriers
  while omitting the global field.
- Default all output contains global barriers exactly once at the top level.
- `all --exclude-global-barriers` preserves every effective scope and omits the
  top-level global field.
- Combining exclusion with `--include-expired` affects only local barriers.
- Classic unified GC and NextGen keyspace-level GC behavior remain unchanged.

The tests remove every `gc-state global` invocation, output type, and assertion.
PR #11117 already proves server-side snapshot consistency and verifies that
excluded requests stay on the no-global-read path, so this PR does not duplicate
those failpoint tests.

## Documentation and PR updates

The user documentation and PR metadata must describe the final two-command
workflow.

The `tools/pd-ctl/README.md` update makes these changes:

- Remove the standalone global view and its JSON example.
- Add `global_gc_barriers` to the keyspace JSON example.
- Explain missing versus present-empty global barrier fields.
- Document `--exclude-global-barriers` for both remaining subcommands.
- Explain its interaction with `--include-expired`.
- Recommend `keyspace` for one scope and reserve `all` for full-cluster
  inspection.

The PR body and release note describe `keyspace` and `all`, the enhanced
`GetGCState` path, and the shared exclusion flag. They do not claim that the PR
adds a standalone global command.

## Implementation sequence

The implementation follows this order to isolate rebase work from command
behavior changes:

1. Rebase the branch onto the `upstream/master` commit that contains PR #11117.
2. Resolve the GC client model and conversion conflicts, and run focused client
   tests.
3. Update command unit tests for the two-command tree, shared flag, option
   mapping, JSON presence contract, and compatibility errors.
4. Refactor the reader, projections, and Cobra commands until the unit tests
   pass.
5. Update the real PD safepoint integration test for the complete flag matrix.
6. Update the README, help contract, PR body, and release note.
7. Run formatting, focused client and pd-ctl tests, Classic and NextGen
   integration tests, the `pd-ctl` build, and relevant static checks.
8. Confirm failpoints are disabled and the worktree contains no generated or
   unrelated files before updating the PR.

## Acceptance criteria

The refactor is complete when the implementation meets every observable
contract in this design.

- `gc-state global` is absent from command registration, implementation, tests,
  help, documentation, and PR metadata.
- Default `gc-state keyspace` obtains local and global barriers with one
  `GetGCState` call.
- No code calls `GetAllKeyspacesGCStates` solely to obtain global barriers.
- `gc-state all` remains the only full-keyspace enumeration path.
- `--exclude-global-barriers` controls both the RPC option and JSON field in
  `keyspace` and `all`.
- A requested empty list encodes as `[]`, while explicit exclusion omits the
  field.
- Expired filtering works consistently for every requested barrier type.
- A server that omits requested global barriers produces an actionable error
  and supports the explicit degraded view.
- Client conversion preserves keyspace-level GC metadata and optional global
  barriers at the same time.
- Focused unit, Classic integration, NextGen integration, build, and static
  checks pass.

## Next steps

After this design is reviewed, create a detailed implementation plan with
file-level edits, test-first steps, verification commands, and review
checkpoints. Do not change product code before that plan is approved.
