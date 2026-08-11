# GC State Global Barrier Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `pd-ctl gc-state keyspace` use `GetGCState` to return local and
global GC barriers, remove `gc-state global`, and let both remaining views skip
global barriers with `--exclude-global-barriers`.

**Architecture:** Rebase onto the `GetGCState` global barrier support from PR
#11117, preserve the current PR's keyspace-level GC metadata, and pass a single
positive inclusion boolean through the command reader. Model optional JSON
output with a pointer to a slice so the CLI distinguishes an omitted field from
a requested empty list.

**Tech Stack:** Go 1.25, Cobra, `github.com/tikv/pd/client/clients/gc`,
`encoding/json`, Testify, PD test clusters, and Make.

## Global constraints

These constraints apply to every task in this plan.

- Read and follow the repository `AGENTS.md` before changing files.
- Use Go 1.25 or later. CI uses Go 1.25.
- Start from a clean worktree with failpoints disabled.
- Rebase onto an `upstream/master` that contains commit `3430f76dc` from PR
  #11117 before editing command behavior.
- Do not add dependencies or edit module files beyond conflict resolution with
  the versions already merged by PR #11117.
- Keep `gc-state keyspace` to one `GetGCState` RPC. Do not add an automatic
  `GetAllKeyspacesGCStates` fallback.
- Keep `GetAllKeyspacesGCStates` only for the `all` view.
- Register only `keyspace` and `all`; do not retain a hidden or deprecated
  `global` alias.
- Define `--exclude-global-barriers` on the parent command so both subcommands
  inherit it.
- Preserve the three global barrier states: omitted, requested-empty, and
  requested-populated.
- Preserve nil barrier filtering, zero-TTL filtering, deterministic sorting,
  effective scope filtering, and reader closure behavior.
- Use `status.Code(err)` on the original error for gRPC compatibility checks.
- Use `gofmt` and the repository import order on every touched Go file.
- Never edit files or run non-test commands while failpoints are enabled.
- Disable failpoints immediately after each failpoint-enabled test, including
  after a failed test.
- Use signed commits with subjects no longer than 70 characters and bodies
  wrapped at 80 characters.
- Do not hard-wrap prose when editing the GitHub PR body.

The approved design is in
[`../specs/2026-08-11-gc-state-global-barrier-refactor-design.md`](../specs/2026-08-11-gc-state-global-barrier-refactor-design.md).

---

## File map

The implementation modifies existing GC client, command, test, and user
documentation files. It does not add a new production source file.

- `client/clients/gc/client.go` retains `IsKeyspaceLevelGC` alongside PR
  #11117's optional global barrier fields and accessors.
- `client/gc_client.go` preserves keyspace-level mode through local and global
  protobuf conversion.
- `client/gc_client_test.go` locks the composed conversion behavior.
- `tests/integrations/client/client_test.go` retains both sides' integration
  assertions while resolving the rebase.
- `tools/pd-ctl/pdctl/command/gc_state_command.go` owns reader options, JSON
  projection, flags, routing, and errors.
- `tools/pd-ctl/pdctl/command/gc_state_command_test.go` owns projection,
  option, command, error, and help contracts.
- `tools/pd-ctl/tests/safepoint/gc_state_test.go` owns end-to-end Classic and
  NextGen command behavior against a real PD server.
- `tools/pd-ctl/README.md` documents the final two-command workflow.

## Task 1: Rebase and compose the GC client model

This task establishes the correct baseline and combines PR #11117's optional
global barriers with PR #11054's keyspace-level GC metadata.

**Files:**

- Modify during conflict resolution: `client/clients/gc/client.go:285-380`
- Modify during conflict resolution: `client/gc_client.go:298-390`
- Modify: `client/gc_client_test.go:1-90`
- Modify during conflict resolution:
  `tests/integrations/client/client_test.go:2069-2780`
- Review only: `go.mod`, `go.sum`, `client/go.mod`, `client/go.sum`,
  `tests/integrations/go.mod`, `tests/integrations/go.sum`, `tools/go.mod`, and
  `tools/go.sum`

**Interfaces:**

- Consumes: PR #11117's `gc.GCState.WithGlobalGCBarriers`,
  `gc.GCState.HasGlobalGCBarriers`, and
  `gc.GCState.GetGlobalGCBarriers` methods.
- Produces: `gc.GCState.IsKeyspaceLevelGC bool` on states with or without local
  and global barriers.
- Produces: `pbToGCStateWithGlobalGCBarriers(*pdpb.GCState,
  *pdpb.GlobalGCBarriersInfo, time.Time, bool) gc.GCState` that preserves the
  keyspace-level flag.

- [ ] **Step 1: Verify the pre-rebase state**

Run these commands before rewriting branch history:

```bash
make failpoint-disable
git status --short --branch
git log -1 --oneline upstream/master
git merge-base --is-ancestor 3430f76dc upstream/master
```

Expected: failpoints are disabled, the worktree is clean, the ancestor check
exits with status 0, and `upstream/master` contains PR #11117. Stop if the
worktree is dirty; do not stash or discard user changes automatically.

- [ ] **Step 2: Refresh and rebase onto upstream master**

Run:

```bash
git fetch upstream master
git rebase upstream/master
```

Expected: the rebase can stop in the four client files listed above because
both PRs modify the GC state model and protobuf conversion. Resolve only those
semantic overlaps. Do not resolve an entire file with `--ours` or `--theirs`.

- [ ] **Step 3: Resolve the public `GCState` model composition**

Ensure the resolved struct in `client/clients/gc/client.go` contains both the
public mode field and PR #11117's private global barrier state:

```go
type GCState struct {
	// The ID of the keyspace this GC state belongs to.
	KeyspaceID uint32
	// IsKeyspaceLevelGC reports whether this state belongs to an independent
	// keyspace-level GC scope.
	IsKeyspaceLevelGC bool
	TxnSafePoint      uint64
	GCSafePoint       uint64
	hasGCBarriers     bool
	gcBarriers        []*GCBarrierInfo

	hasGlobalGCBarriers bool
	globalGCBarriers    []*GlobalGCBarrierInfo
}
```

Keep `WithGlobalGCBarriers`, `HasGlobalGCBarriers`, and
`GetGlobalGCBarriers` exactly as merged by PR #11117.

- [ ] **Step 4: Resolve protobuf conversion without losing mode metadata**

In `client/gc_client.go`, keep PR #11117's local conversion and add the mode
assignment immediately before `pbToGCState` returns:

```go
func pbToGCState(
	pb *pdpb.GCState,
	reqStartTime time.Time,
	excludeGCBarriers bool,
) gc.GCState {
	keyspaceID := constants.NullKeyspaceID
	if pb.KeyspaceScope != nil {
		keyspaceID = pb.KeyspaceScope.GetKeyspaceId()
	}

	var state gc.GCState
	if excludeGCBarriers {
		state = gc.NewGCStateWithoutGCBarriers(
			keyspaceID,
			pb.GetTxnSafePoint(),
			pb.GetGcSafePoint(),
		)
	} else {
		gcBarriers := make([]*gc.GCBarrierInfo, 0, len(pb.GetGcBarriers()))
		for _, barrier := range pb.GetGcBarriers() {
			gcBarriers = append(
				gcBarriers,
				pbToGCBarrierInfo(barrier, reqStartTime),
			)
		}
		state = gc.NewGCStateWithGCBarriers(
			keyspaceID,
			pb.GetTxnSafePoint(),
			pb.GetGcSafePoint(),
			gcBarriers,
		)
	}
	state.IsKeyspaceLevelGC = pb.GetIsKeyspaceLevelGc()
	return state
}
```

Keep `pbToGCStateWithGlobalGCBarriers` based on `result := pbToGCState(...)`
and `return result.WithGlobalGCBarriers(barriers)`. That value-receiver flow
preserves `IsKeyspaceLevelGC`.

- [ ] **Step 5: Add a focused regression test for the composed conversion**

Append this test to `client/gc_client_test.go`:

```go
func TestPBToGCStateWithGlobalBarriersPreservesKeyspaceLevelGC(t *testing.T) {
	requestStart := time.Unix(100, 0)
	state := pbToGCStateWithGlobalGCBarriers(
		&pdpb.GCState{
			KeyspaceScope:     wrapKeyspaceScope(42),
			IsKeyspaceLevelGc: true,
			TxnSafePoint:      100,
			GcSafePoint:       90,
		},
		&pdpb.GlobalGCBarriersInfo{
			Barriers: []*pdpb.GlobalGCBarrierInfo{
				{BarrierId: "snapshot", BarrierTs: 95, TtlSeconds: 60},
			},
		},
		requestStart,
		true,
	)

	require.True(t, state.IsKeyspaceLevelGC)
	require.False(t, state.HasGCBarriers())
	require.True(t, state.HasGlobalGCBarriers())
	barriers, err := state.GetGlobalGCBarriers()
	require.NoError(t, err)
	require.Len(t, barriers, 1)
	require.Equal(t, "snapshot", barriers[0].BarrierID)
}
```

- [ ] **Step 6: Run the focused client tests**

Run:

```bash
cd client && go test . -run '^(TestPBToGCStatePreservesKeyspaceLevelGC|TestPBToGCStateWithGlobalBarriersPreservesKeyspaceLevelGC)$' -count=1
```

Expected: PASS. A missing `IsKeyspaceLevelGC` field or a converter that
reconstructs the state after setting the field causes a compile or assertion
failure.

- [ ] **Step 7: Review dependency conflict resolution**

Run:

```bash
git diff upstream/master...HEAD -- go.mod go.sum client/go.mod client/go.sum tests/integrations/go.mod tests/integrations/go.sum tools/go.mod tools/go.sum
```

Expected: the branch uses the kvproto version already present in
`upstream/master`; this task adds no new dependency version.

- [ ] **Step 8: Commit the focused composition regression**

Stage only the intentional post-rebase client changes and test:

```bash
git add client/clients/gc/client.go client/gc_client.go client/gc_client_test.go tests/integrations/client/client_test.go
git diff --cached --check
git diff --cached
git commit -s -m "client: preserve GC mode with global barriers"
```

Expected: the commit contains the composed model/converter and focused
regression only. Replayed rebase commits remain separate history.

## Task 2: Add optional global barriers to JSON projections

This task changes only output construction. It keeps the existing command tree
temporarily so projection behavior can be reviewed independently from routing.

**Files:**

- Modify: `tools/pd-ctl/pdctl/command/gc_state_command.go:148-315`
- Test: `tools/pd-ctl/pdctl/command/gc_state_command_test.go:106-325`

**Interfaces:**

- Consumes: `gc.GCState.HasGlobalGCBarriers()` and
  `gc.GCState.GetGlobalGCBarriers()` from Task 1.
- Produces: `newKeyspaceGCStateOutput(uint32, gc.GCState, bool, bool)
  (keyspaceGCStateOutput, error)`.
- Produces: `newAllGCStatesOutput(gc.ClusterGCStates, bool, bool)
  (allGCStatesOutput, error)`.
- Produces: optional `GlobalGCBarriers *[]gcBarrierOutput` fields on both
  top-level output types.

- [ ] **Step 1: Write failing keyspace projection tests**

Add this test next to `TestNewKeyspaceGCStateOutput`:

```go
func TestNewKeyspaceGCStateOutputGlobalBarrierPresence(t *testing.T) {
	t.Run("requested-empty", func(t *testing.T) {
		state := gc.NewGCStateWithGCBarriers(42, 100, 90, nil).
			WithGlobalGCBarriers(nil)
		got, err := newKeyspaceGCStateOutput(42, state, false, true)
		require.NoError(t, err)
		require.NotNil(t, got.GlobalGCBarriers)
		require.Empty(t, *got.GlobalGCBarriers)
		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"global_gc_barriers":[]`)
	})

	t.Run("excluded", func(t *testing.T) {
		state := gc.NewGCStateWithGCBarriers(42, 100, 90, nil)
		got, err := newKeyspaceGCStateOutput(42, state, false, false)
		require.NoError(t, err)
		require.Nil(t, got.GlobalGCBarriers)
		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "global_gc_barriers")
	})

	t.Run("requested-missing", func(t *testing.T) {
		state := gc.NewGCStateWithGCBarriers(42, 100, 90, nil)
		_, err := newKeyspaceGCStateOutput(42, state, false, true)
		require.ErrorContains(t, err,
			"retry with --exclude-global-barriers")
	})
}
```

- [ ] **Step 2: Write failing all-view projection tests**

Add this test next to `TestNewAllGCStatesOutputKeepsEmptyGlobalBarrierArray`:

```go
func TestNewAllGCStatesOutputGlobalBarrierPresence(t *testing.T) {
	t.Run("requested-empty", func(t *testing.T) {
		state := gc.NewClusterGCStatesWithGlobalGCBarriers(
			map[uint32]gc.GCState{},
			nil,
		)
		got, err := newAllGCStatesOutput(state, false, true)
		require.NoError(t, err)
		require.NotNil(t, got.GlobalGCBarriers)
		require.Empty(t, *got.GlobalGCBarriers)
		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"global_gc_barriers":[]`)
	})

	t.Run("excluded", func(t *testing.T) {
		state := gc.NewClusterGCStatesWithoutGlobalGCBarriers(
			map[uint32]gc.GCState{},
		)
		got, err := newAllGCStatesOutput(state, false, false)
		require.NoError(t, err)
		require.Nil(t, got.GlobalGCBarriers)
		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "global_gc_barriers")
	})

	t.Run("requested-missing", func(t *testing.T) {
		state := gc.NewClusterGCStatesWithoutGlobalGCBarriers(
			map[uint32]gc.GCState{},
		)
		_, err := newAllGCStatesOutput(state, false, true)
		require.ErrorContains(t, err,
			"retry with --exclude-global-barriers")
	})
}
```

- [ ] **Step 3: Run the projection tests to verify they fail**

Run:

```bash
cd tools && go test ./pd-ctl/pdctl/command -run '^(TestNewKeyspaceGCStateOutputGlobalBarrierPresence|TestNewAllGCStatesOutputGlobalBarrierPresence)$' -count=1
```

Expected: FAIL to compile because the output structs lack
`GlobalGCBarriers` and the converters accept only three and two arguments.

- [ ] **Step 4: Add optional fields to both output structs**

Replace the two output types with:

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

Do not add `omitempty` to local barrier fields.

- [ ] **Step 5: Implement keyspace global barrier projection**

Replace `newKeyspaceGCStateOutput` with:

```go
func newKeyspaceGCStateOutput(
	requestedKeyspaceID uint32,
	state gc.GCState,
	includeExpired bool,
	includeGlobalGCBarriers bool,
) (keyspaceGCStateOutput, error) {
	barriers, err := state.GetGCBarriers()
	if err != nil {
		return keyspaceGCStateOutput{}, errors.Annotatef(
			err,
			"failed to read GC barriers for keyspace %d",
			requestedKeyspaceID,
		)
	}

	var globalOutput *[]gcBarrierOutput
	if includeGlobalGCBarriers {
		if !state.HasGlobalGCBarriers() {
			return keyspaceGCStateOutput{}, errors.Errorf(
				"gc-state keyspace requires a PD server whose GetGCState " +
					"supports global GC barriers; retry with " +
					"--exclude-global-barriers",
			)
		}
		globalBarriers, err := state.GetGlobalGCBarriers()
		if err != nil {
			return keyspaceGCStateOutput{}, errors.WithStack(err)
		}
		converted := newGlobalGCBarrierOutputs(
			globalBarriers,
			includeExpired,
		)
		globalOutput = &converted
	}

	return keyspaceGCStateOutput{
		RequestedKeyspaceID: requestedKeyspaceID,
		EffectiveKeyspaceID: state.KeyspaceID,
		IsKeyspaceLevelGC:   state.IsKeyspaceLevelGC,
		TxnSafePoint:        state.TxnSafePoint,
		GCSafePoint:         state.GCSafePoint,
		GCBarriers: newLocalGCBarrierOutputs(
			barriers,
			includeExpired,
		),
		GlobalGCBarriers: globalOutput,
	}, nil
}
```

- [ ] **Step 6: Implement all-view global barrier projection**

Keep the existing state conversion and sorting loop in
`newAllGCStatesOutput`. Replace its final global conversion and return block
with:

```go
var globalOutput *[]gcBarrierOutput
if includeGlobalGCBarriers {
	if !clusterState.HasGlobalGCBarriers() {
		return allGCStatesOutput{}, errors.New(
			"gc-state all response does not include global GC barriers; " +
				"retry with --exclude-global-barriers",
		)
	}
	globalBarriers, err := clusterState.GetGlobalGCBarriers()
	if err != nil {
		return allGCStatesOutput{}, errors.WithStack(err)
	}
	converted := newGlobalGCBarrierOutputs(globalBarriers, includeExpired)
	globalOutput = &converted
}

return allGCStatesOutput{
	GCStates:         states,
	GlobalGCBarriers: globalOutput,
}, nil
```

Add `includeGlobalGCBarriers bool` as the third parameter. Until Task 3 removes
the global subcommand and adds the flag, pass `true` from the existing
`keyspace` and `all` command call sites so the package compiles.

- [ ] **Step 7: Update existing projection assertions**

Update existing calls to the converters with the new boolean. Dereference
`GlobalGCBarriers` only after `require.NotNil`. Delete
`TestNewGlobalGCStateOutputSortsAndKeepsEmptyArray` only in Task 3, when its
production converter is removed. For existing included keyspace cases, attach
the expected globals with `state.WithGlobalGCBarriers(...)`; for excluded
cases, deliberately use a state without global data and pass `false`.

Update `TestGCStateOutputRejectsExcludedBarriers` to keep the missing-local
assertion, assert the exact actionable error for a requested but missing global
result, and add successful excluded projections that omit the field. This
locks the distinction between missing server capability and explicit user
exclusion.

- [ ] **Step 8: Run all projection tests**

Run:

```bash
cd tools && go test ./pd-ctl/pdctl/command -run '^(TestNew(Keyspace|All)GCStates?Output.*|TestGCStateOutputRejectsExcludedBarriers)$' -count=1
```

Expected: PASS. The encoded present-empty cases contain `[]`, and excluded
cases omit the field.

- [ ] **Step 9: Commit the projection change**

Run:

```bash
git add tools/pd-ctl/pdctl/command/gc_state_command.go tools/pd-ctl/pdctl/command/gc_state_command_test.go
git diff --cached --check
git diff --cached
git commit -s -m "pd-ctl: project optional global GC barriers"
```

Expected: the commit changes projection types and converters but does not yet
remove command routing.

## Task 3: Add the shared flag and remove the global command

This task changes command routing and reader options after projection behavior
is independently covered.

**Files:**

- Modify: `tools/pd-ctl/pdctl/command/gc_state_command.go:37-103`
- Modify: `tools/pd-ctl/pdctl/command/gc_state_command.go:317-474`
- Test: `tools/pd-ctl/pdctl/command/gc_state_command_test.go:38-72`
- Test: `tools/pd-ctl/pdctl/command/gc_state_command_test.go:326-731`

**Interfaces:**

- Consumes: both optional projection signatures from Task 2.
- Produces: `gcStateAPIOptions(bool) []gc.GCStatesAPIOption`.
- Produces: `gcStateReader.getGCState(context.Context, uint32, bool)`.
- Produces: `gcStateReader.getAllKeyspacesGCStates(context.Context, bool)`.
- Produces: parent flag `--exclude-global-barriers`, default `false`.

- [ ] **Step 1: Refactor the fake reader for failing routing tests**

Replace its global call counter with one inclusion field, delete
`getGlobalGCState`, and use these signatures:

```go
type fakeGCStateReader struct {
	state                   gc.GCState
	clusterState            gc.ClusterGCStates
	err                     error
	requestedID             uint32
	includeGlobalGCBarriers bool
	getStateCalls           int
	getAllCalls             int
	closed                  bool
}

func (r *fakeGCStateReader) getGCState(
	_ context.Context,
	keyspaceID uint32,
	includeGlobalGCBarriers bool,
) (gc.GCState, error) {
	r.requestedID = keyspaceID
	r.includeGlobalGCBarriers = includeGlobalGCBarriers
	r.getStateCalls++
	return r.state, r.err
}

func (r *fakeGCStateReader) getAllKeyspacesGCStates(
	_ context.Context,
	includeGlobalGCBarriers bool,
) (gc.ClusterGCStates, error) {
	r.includeGlobalGCBarriers = includeGlobalGCBarriers
	r.getAllCalls++
	return r.clusterState, r.err
}
```

The production package now fails to compile until its interface and call sites
match.

- [ ] **Step 2: Write a failing client option test**

Replace `TestReadClusterGCStatesOptions` and its fake client with:

```go
func TestGCStateAPIOptions(t *testing.T) {
	for _, includeGlobalGCBarriers := range []bool{false, true} {
		t.Run(strconv.FormatBool(includeGlobalGCBarriers), func(t *testing.T) {
			options := gc.DefaultGCStatesAPIOptions()
			for _, option := range gcStateAPIOptions(
				includeGlobalGCBarriers,
			) {
				option(&options)
			}
			require.False(t, options.ExcludeGCBarriers)
			require.Equal(t, !includeGlobalGCBarriers,
				options.ExcludeGlobalGCBarriers)
		})
	}
}
```

Add `strconv` to the standard-library import block before adding this test.

- [ ] **Step 3: Write failing flag and command-tree tests**

Add a table test that executes both subcommands with and without the flag:

```go
func TestGCStateCommandGlobalBarrierFlag(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		args        []string
		wantInclude bool
	}{
		{name: "keyspace-default", args: []string{"keyspace", "42"}, wantInclude: true},
		{name: "keyspace-excluded", args: []string{"keyspace", "42", "--exclude-global-barriers"}},
		{name: "all-default", args: []string{"all"}, wantInclude: true},
		{name: "all-excluded", args: []string{"all", "--exclude-global-barriers"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			reader := &fakeGCStateReader{
				state: gc.NewGCStateWithGCBarriers(42, 100, 90, nil).
					WithGlobalGCBarriers(nil),
				clusterState: gc.NewClusterGCStatesWithGlobalGCBarriers(
					map[uint32]gc.GCState{},
					nil,
				),
			}
			cmd := buildGCStateCommand(func(*cobra.Command) (gcStateReader, error) {
				return reader, nil
			})
			output := new(bytes.Buffer)
			cmd.SetOut(output)
			cmd.SetErr(output)
			cmd.SetArgs(testCase.args)

			require.NoError(t, cmd.Execute())
			require.Equal(t, testCase.wantInclude,
				reader.includeGlobalGCBarriers)
			if testCase.wantInclude {
				require.Contains(t, output.String(), "global_gc_barriers")
			} else {
				require.NotContains(t, output.String(), "global_gc_barriers")
			}
		})
	}
}
```

Add an explicit removal test:

```go
func TestGCStateGlobalCommandIsRemoved(t *testing.T) {
	factoryCalled := false
	cmd := buildGCStateCommand(func(*cobra.Command) (gcStateReader, error) {
		factoryCalled = true
		return nil, errors.New("factory must not run")
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"global"})

	err := cmd.Execute()
	require.ErrorContains(t, err, `unknown command "global"`)
	require.False(t, factoryCalled)
}
```

- [ ] **Step 4: Run the routing tests to verify they fail**

Run:

```bash
cd tools && go test ./pd-ctl/pdctl/command -run '^(TestGCStateAPIOptions|TestGCStateCommandGlobalBarrierFlag|TestGCStateGlobalCommandIsRemoved)$' -count=1
```

Expected: FAIL because `gcStateAPIOptions` and the exclusion flag do not exist,
the production reader uses old signatures, and `global` remains registered.

- [ ] **Step 5: Replace the reader interface and option construction**

Use this interface and helper in `gc_state_command.go`:

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

func gcStateAPIOptions(
	includeGlobalGCBarriers bool,
) []gc.GCStatesAPIOption {
	return []gc.GCStatesAPIOption{
		gc.ExcludeGCBarriers(false),
		gc.ExcludeGlobalGCBarriers(!includeGlobalGCBarriers),
	}
}
```

Update both concrete reader methods to accept the boolean and call the bound
client with `gcStateAPIOptions(includeGlobalGCBarriers)...`. Delete
`clusterGCStatesClient`, `readClusterGCStates`, and `getGlobalGCState`.

- [ ] **Step 6: Add and read the persistent flag**

Define the flag next to `gcStateIncludeExpiredFlag`:

```go
const (
	gcStateIncludeExpiredFlag         = "include-expired"
	gcStateExcludeGlobalBarriersFlag = "exclude-global-barriers"
)
```

Add this getter:

```go
func getGCStateIncludeGlobalGCBarriers(
	cmd *cobra.Command,
) (bool, error) {
	excludeGlobalGCBarriers, err := cmd.Flags().GetBool(
		gcStateExcludeGlobalBarriersFlag,
	)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return !excludeGlobalGCBarriers, nil
}
```

Register it on the parent command:

```go
command.PersistentFlags().Bool(
	gcStateExcludeGlobalBarriersFlag,
	false,
	"exclude global GC barriers from the PD request and JSON output",
)
```

- [ ] **Step 7: Route the boolean through both remaining commands**

In both `RunE` functions, read `includeExpired` and then
`includeGlobalGCBarriers` before creating the reader. Use these exact call
shapes:

```go
state, err := reader.getGCState(
	cmd.Context(),
	keyspaceID,
	includeGlobalGCBarriers,
)
```

```go
output, err := newKeyspaceGCStateOutput(
	keyspaceID,
	state,
	includeExpired,
	includeGlobalGCBarriers,
)
```

```go
clusterState, err := reader.getAllKeyspacesGCStates(
	cmd.Context(),
	includeGlobalGCBarriers,
)
```

```go
output, err := newAllGCStatesOutput(
	clusterState,
	includeExpired,
	includeGlobalGCBarriers,
)
```

Keep existing RPC error annotations and original-error `status.Code` checks.

- [ ] **Step 8: Remove the standalone global command**

Delete `newGCStateGlobalCommand`, `globalGCStateOutput`, and
`newGlobalGCStateOutput`. Register only:

```go
command.AddCommand(
	newGCStateKeyspaceCommand(factory),
	newGCStateAllCommand(factory),
)
```

Delete `TestGCStateGlobalCommand`, the obsolete global converter test, every
`global-*` expired-visibility row, every `global-*` error row, and the
`{"global", "extra"}` validation row. Do not retain a test invocation that
could make the removed subcommand look supported.

Update the remaining command tests as follows:

- `TestGCStateKeyspaceCommand` supplies a state with requested-empty globals,
  requires `global_gc_barriers`, and proves one `getStateCalls` and zero
  `getAllCalls`.
- `TestGCStateCommandExpiredBarrierVisibility` attaches the active and expired
  global fixtures to the keyspace state, then checks globals for both keyspace
  rows as well as both all rows.
- `TestGCStateCommandErrors` adds a `single-missing-global-barriers` case whose
  state has local barriers but no global wrapper and expects
  `retry with --exclude-global-barriers`. Update the all-view missing-global
  case to expect the same remediation.
- `TestGCStateCommandReturnsOutputError` supplies requested-empty globals so
  execution reaches the failing writer.

- [ ] **Step 9: Update the help contract**

Use help copy that states the final behavior:

```go
Short: "show keyspace and cluster-wide GC state",
Long: "Show effective per-keyspace GC safe points and local and global " +
	"barriers. Expired barriers awaiting lazy deletion are hidden by " +
	"default; use --include-expired to include zero-TTL barriers returned " +
	"by PD. Use keyspace for one effective GC scope or all for every " +
	"effective GC scope.",
```

The keyspace long help must say that it includes local and global barriers and
that `--exclude-global-barriers` omits cluster-wide barriers. The all long help
must say that global barriers appear once at the top level and that the same
flag omits them. Remove every recommendation to run `gc-state global`.

In `TestGCStateCommandHelpContract`, assert both persistent flags have default
`false`, assert the exact exclusion usage string, and assert that the command's
children are exactly `all` and `keyspace` after Cobra sorts them.

- [ ] **Step 10: Run the complete command unit suite**

Run:

```bash
cd tools && go test ./pd-ctl/pdctl/command -run 'GCState' -count=1
```

Expected: PASS. The default cases include an empty global array, exclusion
cases omit it, and `global` is rejected before client creation.

- [ ] **Step 11: Commit command routing and flag behavior**

Run:

```bash
git add tools/pd-ctl/pdctl/command/gc_state_command.go tools/pd-ctl/pdctl/command/gc_state_command_test.go
git diff --cached --check
git diff --cached
git commit -s -m "pd-ctl: use GetGCState for global barriers"
```

Expected: this commit contains the reader refactor, shared flag, command
removal, compatibility paths, and command tests.

## Task 4: Update real PD command coverage

This task verifies the complete behavior against a real Classic or NextGen PD
server without duplicating PR #11117's server failpoint tests.

**Files:**

- Test: `tools/pd-ctl/tests/safepoint/gc_state_test.go:42-66`
- Test: `tools/pd-ctl/tests/safepoint/gc_state_test.go:218-388`

**Interfaces:**

- Consumes: the final `keyspace`, `all`, `--include-expired`, and
  `--exclude-global-barriers` command contracts from Task 3.
- Produces: end-to-end coverage for default, expired, excluded, Classic, and
  NextGen output.

- [ ] **Step 1: Update integration output types**

Add global barriers to the single-keyspace decoder:

```go
type gcStateCommandSingle struct {
	RequestedKeyspaceID uint32                  `json:"requested_keyspace_id"`
	EffectiveKeyspaceID uint32                  `json:"effective_keyspace_id"`
	IsKeyspaceLevelGC   bool                    `json:"is_keyspace_level_gc"`
	TxnSafePoint        uint64                  `json:"txn_safe_point"`
	GCSafePoint         uint64                  `json:"gc_safe_point"`
	GCBarriers          []gcStateCommandBarrier `json:"gc_barriers"`
	GlobalGCBarriers    []gcStateCommandBarrier `json:"global_gc_barriers"`
}
```

Delete `gcStateCommandGlobal`.

- [ ] **Step 2: Make the default keyspace assertions require global barriers**

Replace the default `NotContains` assertion with:

```go
re.Contains(singleProperties, "global_gc_barriers")
requireGCStateCommandBarriers(
	re,
	keyspaceLevelResponse.GlobalGCBarriers,
	[]expectedGCStateCommandBarrier{
		{barrierID: "a-global", barrierTS: 310},
		{barrierID: "z-global", barrierTS: 320},
	},
)
```

Add the same active global barrier expectation to the NullKeyspace and Classic
unified-GC keyspace responses. This proves the global result is independent of
the requested keyspace.

- [ ] **Step 3: Extend the keyspace expired assertion**

After decoding `keyspaceLevelWithExpired`, assert:

```go
requireGCStateCommandBarriers(
	re,
	keyspaceLevelWithExpired.GlobalGCBarriers,
	[]expectedGCStateCommandBarrier{
		{barrierID: "a-global", barrierTS: 310},
		{barrierID: "z-global", barrierTS: 320},
		{barrierID: "expired-global", barrierTS: 330, expired: true},
	},
)
```

- [ ] **Step 4: Add keyspace exclusion coverage**

Execute the excluded command and inspect raw field presence:

```go
output, err = tests.ExecuteCommand(
	ctl.GetRootCmd(),
	"-u",
	pdAddr,
	"gc-state",
	"keyspace",
	keyspaceLevelIDString,
	"--exclude-global-barriers",
)
re.NoError(err)
var excludedKeyspaceProperties map[string]json.RawMessage
re.NoError(json.Unmarshal(output, &excludedKeyspaceProperties), string(output))
re.NotContains(excludedKeyspaceProperties, "global_gc_barriers")
var excludedKeyspace gcStateCommandSingle
re.NoError(json.Unmarshal(output, &excludedKeyspace), string(output))
requireGCStateCommandBarriers(
	re,
	excludedKeyspace.GCBarriers,
	[]expectedGCStateCommandBarrier{
		{barrierID: "a-local", barrierTS: 210},
		{barrierID: "z-local", barrierTS: 220, expires: true},
	},
)
```

Repeat with both flags and assert `expired-local` appears while the global
field remains absent.

- [ ] **Step 5: Add all-view exclusion coverage**

Execute:

```go
output, err = tests.ExecuteCommand(
	ctl.GetRootCmd(),
	"-u",
	pdAddr,
	"gc-state",
	"all",
	"--exclude-global-barriers",
)
re.NoError(err)
var excludedAllProperties map[string]json.RawMessage
re.NoError(json.Unmarshal(output, &excludedAllProperties), string(output))
re.Contains(excludedAllProperties, "gc_states")
re.NotContains(excludedAllProperties, "global_gc_barriers")
```

Decode `gc_states` and compare the NullKeyspace and keyspace-level entries to
the default all view. Repeat with `--include-expired` and assert only local
expired barriers are added.

- [ ] **Step 6: Delete standalone global command coverage**

Delete the command calls and assertions from the current global block,
including `globalProperties`, `global`, `globalWithExpired`, and every
`gcStateCommandGlobal` use.

- [ ] **Step 7: Run the Classic integration test**

Enable failpoints only around the test:

```bash
make failpoint-enable
cd tools && go test ./pd-ctl/tests/safepoint -run '^TestGCState$' -count=1
cd .. && make failpoint-disable
```

Expected: PASS. If the test fails, return to the repository root and run
`make failpoint-disable` before diagnosing or editing.

- [ ] **Step 8: Run the NextGen integration test**

Run:

```bash
make failpoint-enable
cd tools && go test -tags nextgen ./pd-ctl/tests/safepoint -run '^TestGCState$' -count=1
cd .. && make failpoint-disable
```

Expected: PASS with the system keyspace retained as a keyspace-level GC scope.
If the test fails, disable failpoints before any other action.

- [ ] **Step 9: Commit real PD coverage**

Run:

```bash
git add tools/pd-ctl/tests/safepoint/gc_state_test.go
git diff --cached --check
git diff --cached
git commit -s -m "test: cover optional global GC state output"
```

Expected: the commit changes only the real PD test and removes all standalone
global command coverage.

## Task 5: Update the user workflow documentation

This task updates user-facing documentation after the executable behavior and
integration tests are stable. Use the `docs-writer` skill for this task.

**Files:**

- Modify: `tools/pd-ctl/README.md:14-130`

**Interfaces:**

- Consumes: final command and JSON contracts from Tasks 2 through 4.
- Produces: one documented workflow for a selected keyspace and one for all
  effective scopes.

- [ ] **Step 1: Remove the standalone global workflow**

Delete the `pd-ctl gc-state global` command example, its JSON object, and prose
that recommends a second command when local barriers do not explain a safe
point.

- [ ] **Step 2: Add global barriers to the keyspace example**

Extend the existing keyspace JSON object with this top-level field:

```json
"global_gc_barriers": [
  {
    "barrier_id": "native_br",
    "barrier_ts": 464940000000000000,
    "ttl_seconds": 9223372036854775807
  }
]
```

State that local and global barriers come from one `GetGCState` read and that
the same global list applies to every keyspace.

- [ ] **Step 3: Document presence and exclusion semantics**

Add prose with these exact behavioral claims:

- The default `keyspace` and `all` views request global barriers.
- An empty `global_gc_barriers` array means PD returned no global barriers.
- `--exclude-global-barriers` skips the read and removes the JSON field.
- The flag applies to both remaining subcommands.
- When combined with `--include-expired`, exclusion wins for global barriers,
  while expired local barriers remain visible.

Include these examples:

```bash
pd-ctl gc-state keyspace 42 --exclude-global-barriers
pd-ctl gc-state all --exclude-global-barriers --include-expired
```

- [ ] **Step 4: Clarify command selection**

Recommend `keyspace` when diagnosing one scope. Reserve `all` for cases that
require every effective GC scope because it enumerates all keyspaces.

- [ ] **Step 5: Verify documentation references**

Run:

```bash
rg -n 'gc-state global|getGlobalGCState|global command' tools/pd-ctl/README.md tools/pd-ctl/pdctl/command
git diff --check
```

Expected: `rg` returns no matches and `git diff --check` passes.

- [ ] **Step 6: Commit the documentation update**

Run:

```bash
git add tools/pd-ctl/README.md
git diff --cached --check
git diff --cached
git commit -s -m "docs: update GC state troubleshooting workflow"
```

Expected: the commit contains only user documentation that matches the tested
command behavior.

## Task 6: Run final verification and update PR metadata

This task proves the refactor across modules and updates PR #11054 only after
the user authorizes the external GitHub change.

**Files:**

- Verify: all files modified by Tasks 1 through 5
- External update after authorization: PR #11054 body

**Interfaces:**

- Consumes: all implementation and documentation commits.
- Produces: a clean worktree, passing focused and repository checks, and PR
  metadata that describes only `keyspace` and `all`.

- [ ] **Step 1: Format and verify no dependency drift**

Run with failpoints disabled:

```bash
make failpoint-disable
gofmt -w client/gc_client.go client/gc_client_test.go client/clients/gc/client.go tools/pd-ctl/pdctl/command/gc_state_command.go tools/pd-ctl/pdctl/command/gc_state_command_test.go tools/pd-ctl/tests/safepoint/gc_state_test.go
git diff --check
git diff -- go.mod go.sum client/go.mod client/go.sum tests/integrations/go.mod tests/integrations/go.sum tools/go.mod tools/go.sum
```

Expected: formatting produces no new semantic diff, whitespace checks pass,
and module files contain only versions inherited from the rebased master.

- [ ] **Step 2: Run focused client and command unit tests**

Run:

```bash
cd client && go test . -run 'GCState' -count=1
cd ../tools && go test ./pd-ctl/pdctl/command -run 'GCState' -count=1
cd ..
```

Expected: PASS in both modules.

- [ ] **Step 3: Run Classic and NextGen real PD tests**

Run only test commands while failpoints are enabled:

```bash
make failpoint-enable
cd tools && go test ./pd-ctl/tests/safepoint -run '^TestGCState$' -count=1
go test -tags nextgen ./pd-ctl/tests/safepoint -run '^TestGCState$' -count=1
cd .. && make failpoint-disable
```

Expected: both runs pass. Regardless of either result, run
`make failpoint-disable` before continuing.

- [ ] **Step 4: Build both pd-ctl variants**

Run:

```bash
make pd-ctl
NEXT_GEN=1 make pd-ctl
```

Expected: both builds succeed.

- [ ] **Step 5: Run repository checks**

Run:

```bash
make check
make basic-test
cd client && make
cd ..
```

Expected: formatting, lint, tidy, error documentation, root tests, and the
client module pipeline pass. If a command enables failpoints internally and
fails, run `make failpoint-disable` before inspecting or editing files.

- [ ] **Step 6: Prove obsolete paths are absent**

Run:

```bash
rg -n 'gc-state global|getGlobalGCState|newGCStateGlobalCommand|globalGCStateOutput' tools/pd-ctl
rg -n 'GetAllKeyspacesGCStates' tools/pd-ctl/pdctl/command/gc_state_command.go
```

Expected: the first command returns no matches. The second command shows only
the all-view reader interface, concrete method, and all command call path.

- [ ] **Step 7: Review the final branch diff and worktree**

Run:

```bash
make failpoint-disable
git status --short --branch
git diff --check upstream/master...HEAD
git diff --stat upstream/master...HEAD
git log --oneline upstream/master..HEAD
```

Expected: no unstaged or untracked artifacts remain, the diff contains only
the approved PR scope, and every new commit has a signed repository-style
message. If formatting changed tracked files in Step 1, fold those changes into
the task that owns them instead of creating an unrelated cleanup commit.

- [ ] **Step 8: Prepare the unwrapped PR body update**

After the user authorizes editing PR #11054, create
`/tmp/pd-11054-refactor-body.md` with this content. Keep prose paragraphs on
single lines because PR Markdown must not be hard-wrapped:

````markdown
### What problem does this PR solve?

PD exposes per-keyspace and cluster-wide GC state through RPCs, but operators cannot inspect that state through `pd-ctl`. This makes it difficult to identify whether GC advancement is blocked by a keyspace-local barrier or a cluster-wide global barrier.

Issue Number: close #11013, ref #8978

### What is changed and how does it work?

```commit-message
Add read-only `pd-ctl gc-state keyspace` and `gc-state all` commands. The keyspace view uses `GetGCState` to return the effective safe points, local barriers, and global barriers in one read. The all view uses `GetAllKeyspacesGCStates` only when every effective GC scope is required. Both views support `--exclude-global-barriers` to skip the global barrier read and omit the JSON field, while `--include-expired` controls zero-TTL barrier visibility.

Expose the server-provided keyspace-level GC mode through the public GC client model so the command can distinguish independent keyspace GC from unified GC. Return deterministic JSON and preserve the distinction between an omitted global barrier result and a requested empty list.
```

### Check List

Tests

- Unit test
- Integration test
- Manual test

### Release note

```release-note
Add `pd-ctl gc-state keyspace` and `gc-state all` commands for inspecting GC safe points and the local and global barriers that can block GC.
```
````

Use `apply_patch` to create the temporary file; do not use shell redirection.

- [ ] **Step 9: Update and verify PR #11054 after authorization**

Run:

```bash
gh pr edit 11054 --repo tikv/pd --body-file /tmp/pd-11054-refactor-body.md
gh pr view 11054 --repo tikv/pd --json title,body,url
```

Expected: the title remains `pd-ctl: add GC state inspection commands`; the
body names only `keyspace` and `all`, includes both flags, and contains the
required issue and release-note blocks. If authorization is not granted, skip
the external update and return the prepared body to the user.

- [ ] **Step 10: Report verification evidence**

Report every command run and its result, the final commit list, any tests that
were skipped with a reason, and whether PR metadata was updated. Do not claim
the refactor is complete if any required check failed or failpoints remain
enabled.

## Execution handoff

Implementation starts only after the user selects an execution approach.
Follow the sub-skill named in the agentic worker header for that approach, keep
the task checkpoints in order, and leave the PR body update approval-gated
because it changes external GitHub state.
