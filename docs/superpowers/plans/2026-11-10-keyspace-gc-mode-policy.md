# Keyspace GC mode policy implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox syntax for tracking. The user explicitly requests coarse work packages, so this plan groups related changes into three tasks.

**Goal:** Enforce immutable effective GC modes, kernel-specific creation, legacy-compatible reads, and diagnostic logging across PD, HTTP clients, and pd-ctl.

**Architecture:** Keyspace Manager owns creation and update validation and one diagnostic observer. A pure shared server resolver supplies Manager, GC routing, enumeration, and index decoding. Existing clients use service results as the authority.

**Tech stack:** Go 1.27.3 or newer, existing etcd transactions, pingcap errors and zap logging, existing failpoint-aware test targets.

**Spec:** [Approved design](../specs/2026-11-10-keyspace-gc-mode-policy-design.md).

## Global constraints

All tasks implement the approved specification. These are the binding cross-task rules.

- A newly created NextGen keyspace uses `keyspace_level`; a newly created Classic keyspace uses `unified`.
- Config updates cannot change the effective GC mode. Repeating a valid value is allowed, including for an existing keyspace whose mode does not match its kernel.
- Config updates reject every mutation of `safe_point_version`, including setting the same value and deleting an absent entry. The field remains readable for legacy compatibility.
- Presence must be checked with a map lookup. An empty string counts as present and suppresses the legacy fallback. A nil config resolves to unified, independently of the kernel. Kernel defaults apply only during creation.
- Existing kernel-incompatible modes remain usable. Existing malformed GC values remain readable and do not prevent updates to unrelated config entries.
- No production bypass, automatic GC-state migration, additional metadata scan, new dependency, or module-version change is permitted. Use `.wt/gc-mode-policy` on `wfxr/gc-mode-policy`; preserve unrelated files and the existing shorter design document there.
- Use the same model tier and reasoning effort for every sub-agent. Implementation agents must not spawn agents. Run implementation agents sequentially because failpoint instrumentation and shared files cannot be mutated concurrently.

## Review focus

These cross-cutting cases must have concrete coverage in the owning task.

- A missing new field with legacy v2 under Classic remains independent GC during unrelated updates; Task 1 covers normalization and Task 2 covers actual GC scope.
- A present invalid or empty new field suppresses legacy fallback but cannot make an unrelated update fail; Task 1 covers the distinction between historical data and requested writes.
- A deletion can activate legacy fallback, and preconditions must observe original metadata before normalization; Task 1 covers both.
- GC index initial loads and watch updates must agree with ordinary and revision-aware GC requests; Task 2 covers both index sources and revision conflicts.
- Concurrent repeated observations must not flood logs or write metadata; Task 1 covers the observer and Task 3 checks its integration.

## Files and shared interfaces

Keep policy and observation logic in focused new files, and integrate with the existing larger files without unrelated restructuring.

| Area | Files |
| --- | --- |
| Policy, diagnostics, and Manager integration | Create `pkg/keyspace/gc_config.go`, `pkg/keyspace/gc_config_test.go`; modify `pkg/keyspace/keyspace.go`, existing keyspace tests as needed |
| Errors | Modify `pkg/errs/errno.go`, regenerate `errors.toml` |
| GC readers and index | Modify `pkg/gc/gc_state_manager.go`, `pkg/gc/enabled_keyspace_index.go`, and their relevant tests; inspect warmup and watcher consumers |
| Service and clients | Modify `server/apiv2/handlers/keyspace.go`, `client/http/interface.go`, `client/http/types.go`, HTTP and client integration tests |
| CLI regression coverage | Modify `tools/pd-ctl/tests/keyspace/keyspace_test.go`; change CLI code only when required by service behavior |
| Historical fixtures | Adjust relevant tests in `pkg/gc`, `tests/server/gc`, `tests/integrations/client`, and `tools/pd-ctl/tests/safepoint` without adding production bypasses |

Task 1 produces `keyspace.EffectiveGCManagementType(config map[string]string) string` and `(*keyspace.Manager).ObserveGCConfig(meta *keyspacepb.KeyspaceMeta)`. The observer returns no error and performs no storage IO. Use a bounded diagnostic cache of 4096 offending keyspaces, scoped to Manager; valid observations clear entries. Reuse existing cache primitives where suitable and preserve concurrency safety.

Task 1 also produces these normalized policy errors in `pkg/errs`: `ErrKeyspaceGCConfigInvalid`, `ErrKeyspaceGCModeMismatch`, `ErrKeyspaceGCModeImmutable`, and `ErrKeyspaceLegacyGCConfigUnsupported`. Task 2 maps them to HTTP 400. Error templates and private helper signatures remain implementer choices.

### Task 1: Manager policy and historical diagnostics

This work package delivers the shared policy and its transactional enforcement, including focused unit coverage and existing keyspace test updates.

**Files:** `pkg/keyspace/gc_config.go`, `pkg/keyspace/gc_config_test.go`, `pkg/keyspace/keyspace.go`, `pkg/keyspace/keyspace_test.go`, other affected `pkg/keyspace/*_test.go`, `pkg/errs/errno.go`, `errors.toml`.

**Interfaces:** Produce the resolver, observer, and normalized errors defined above. Consume the existing Manager transaction, keyspace storage, kernel checks, and preconditions; do not alter GC state storage.

- [x] Add focused failing tests for the resolver truth table, creation input matrix in both kernels, both directions of update rejection, legacy-field mutation rejection, all deletion and normalization cases in the design, original preconditions, failed-update atomicity, historical malformed values, and deduplicated concurrent diagnostics. Run them with failpoints and record the expected failures before implementation.
- [x] Implement creation validation before side effects, reserved-keyspace defaults, transactional update validation and normalization, and diagnostics at existing Manager read and traversal sites. Update historical keyspace fixtures as needed; reads and maintenance operations never normalize data. Add error definitions and regenerate error documentation.
- [x] Run the full `pkg/keyspace` suite in Classic and NextGen and focused error-document/format checks. Self-review the diff for field-presence semantics, preconditions, group side effects, and observer hot-path costs. Report GC/integration fixture fallout for Task 2 rather than weakening the policy. Create one signed-off task commit after these checks.

### Task 2: GC, HTTP, client, and CLI integration

This work package makes every production GC consumer honor the shared policy and updates service-facing coverage and historical GC fixtures together.

**Files:** `pkg/gc/gc_state_manager.go`, `pkg/gc/enabled_keyspace_index.go`, affected `pkg/gc/*_test.go`, `server/apiv2/handlers/keyspace.go`, `tests/server/apiv2/handlers/keyspace_test.go`, `tests/server/gc/gc_test.go`, `client/http/interface.go`, `client/http/types.go`, `client/client_test.go` if parity coverage needs it, `tests/integrations/client/{http_client,keyspace,gc,client}_test.go`, `tools/pd-ctl/tests/keyspace/keyspace_test.go`, `tools/pd-ctl/tests/safepoint/gc_state_test.go` as required.

**Interfaces:** Consume Task 1's resolver, Manager observer, and four policy errors. The index must store the canonical effective mode; ordinary routing and metadata-revision-aware routing must use the same resolver. Preserve the exported client helper's API and existing compatible truth table.

- [x] Add focused failing regression cases for legacy-only v2 routing, full enumeration, index initial load and watch updates, warmup and revision conflicts, both HTTP creation routes, policy HTTP 400 responses, idempotent independent-GC updates, legacy writes, and pd-ctl requests. Preserve the existing stale-cache and storage compatibility assertions.
- [x] Integrate the resolver and observer into GC routing and index decoding, retaining source metadata revision checks and existing cache defenses. Remove only the handler's unconditional `keyspace_level` prohibition, map policy errors consistently, update annotations and stale comments, and adapt historical fixtures using test-only storage setup. Do not replace real endpoint tests with checks of helper functions alone.
- [x] Run the relevant GC, HTTP-handler, HTTP-client, and pd-ctl package suites in both kernels where applicable, with failpoints handled by the correct module's targets. Inspect fixture changes to ensure production creation tests still exercise public validation. Self-review and create one signed-off task commit after the checks pass.

### Task 3: Joint verification and whole-change review

This work package verifies the integrated behavior and resolves concrete failures through the owning implementer. It does not introduce additional functionality.

**Files:** Existing Task 1 and Task 2 files only for verified corrections, plus this plan's progress ledger and reports. No new production area is introduced.

**Interfaces:** Consume the combined implementation and test evidence. Produce a complete verification record, reviewed diff, and any remaining environment limitations.

- [x] Run Classic and NextGen checks for `./pkg/keyspace`, `./pkg/gc`, relevant server GC and keyspace HTTP tests, the client helper, HTTP-client integrations, and pd-ctl keyspace and GC-state tests. Execute commands sequentially because failpoint instrumentation is shared. Use explicit `-tags=nextgen,without_dashboard` for root `gotest`; `NEXT_GEN=1` alone does not pass those tags to that target.
- [x] Run `make check` and the package-scoped basic-test coverage required by repository instructions. Fix actual failures, keep modules unchanged, and disable failpoints before editing or running non-test checks. Record command output and distinguish code failures from missing tools, inaccessible dependencies, or other environment limitations.
- [x] Request a fresh whole-change review for specification compliance and code quality, address material findings through the owning implementer, and rerun only checks justified by changes. Finish with clean intended commits, no generated failpoint files, and a concise handoff with the worktree path and verification results. Do not push or open a PR.

## Execution notes

The user approved the design and explicitly requested delegated implementation. Proceed through this coarse plan without another approval round. Keep progress and any rulings in the plan-specific SDD ledger, preserving task boundaries across context compaction.

Use `GOTOOLCHAIN=auto` to select the installed Go 1.27.3 toolchain. The existing pinned tools are in `/home/wenxuan/dev/pingcap/pd/.tools/bin`; reusing them through `GO_TOOLS_BIN_PATH` is permitted. If `install-tools` only repeats installation of those already available tools, use make's `-o install-tools` option and record that command. Do not skip static checks themselves.

Representative package commands are:

```sh
GOTOOLCHAIN=auto make -o install-tools gotest GO_TOOLS_BIN_PATH=/home/wenxuan/dev/pingcap/pd/.tools/bin GOTEST_ARGS='-tags=without_dashboard ./pkg/keyspace ./pkg/gc -count=1'
GOTOOLCHAIN=auto NEXT_GEN=1 make -o install-tools gotest GO_TOOLS_BIN_PATH=/home/wenxuan/dev/pingcap/pd/.tools/bin GOTEST_ARGS='-tags=nextgen,without_dashboard ./pkg/keyspace ./pkg/gc -count=1'
```

Run an appropriate narrow baseline test before the first product change. Record failures and investigate concrete baseline issues without modifying unrelated code. Stage only intended task files, review staged diffs, use repository-style subjects under 70 characters and signed-off commit messages wrapped at 72 characters, and never include the pre-existing shorter design document or another task's scratch files in a commit.

## Verification results

The implementation at `c044df1d0` passed the following checks. The independent whole-branch review approved the combined changes with no Critical, Important, or Minor findings.

- Complete `pkg/keyspace` and `pkg/gc` suites passed in Classic and NextGen; relevant real server, HTTP/gRPC client, and pd-ctl cases also passed.
- The required Classic package-scoped `basic-test` passed for `pkg/keyspace` and `pkg/gc`.
- Normal default-tag `make check` passed, including every module's configured tidy/static checks, modernize, leakcheck, rule tests, and error documentation generation.
- The optional `without_dashboard` full-lint variant exposes an existing unused parameter at `pkg/dashboard/without_dashboard.go:47`; that unchanged file is outside this patch. No check was suppressed.
- Static verification temporarily restored the matching cached Dashboard asset, checksum-equal to the main checkout, and removed the worktree copy afterwards. No resource download or dependency change was needed.
- Both task reviews and the final whole-branch review passed. Modules remain unchanged and failpoints are disabled.
