# Keyspace GC mode policy

This design makes a persisted keyspace's effective GC mode immutable, restricts newly created keyspaces to the mode supported by their kernel, and preserves legacy metadata through one compatible mode resolver. It covers Manager calls, HTTP APIs, HTTP clients, and pd-ctl.

Status: approved by the user on November 10, 2026. Implementation is authorized through the implementation plan and delegated sub-agents.

Date: November 10, 2026. Code inspection baseline: local `master` at `cbcc61de6`. Function names are the references for implementation because line numbers can change.

## Goals and scope

The change must enforce the following rules for persisted keyspaces. `NullKeyspace` is the existing unified GC scope, has no persisted keyspace metadata, and remains outside the creation policy.

- A newly created NextGen keyspace uses `keyspace_level`; a newly created Classic keyspace uses `unified`.
- Creation rejects any supplied `safe_point_version` entry, including when `gc_management_type` is also supplied. An absent `gc_management_type` receives the kernel default and is explicitly persisted. A present empty, unknown, or kernel-incompatible value is rejected.
- Config updates cannot change the effective GC mode. Repeating a valid value is allowed, including for an existing keyspace whose mode does not match its kernel.
- Config updates reject every mutation of `safe_point_version`, including setting the same value and deleting an absent entry. The field remains readable for legacy compatibility.
- Every supplied non-null `gc_management_type` value in an update must be `unified` or `keyspace_level`. Null means deletion and is subject to the effective-mode comparison.
- A successful config update explicitly persists `gc_management_type` when the resulting config lacks it. This normalization preserves the effective mode and keeps the legacy field unchanged.
- Existing kernel-incompatible modes remain usable. Existing malformed GC values remain readable and do not prevent updates to unrelated config entries. Existing reads and traversals report violations with deduplicated error logs.

This work does not migrate GC state, coordinate GC worker handoff, change GC storage paths, add a migration switch, add a startup scan or periodic watcher, or alter the existing keyspace-state restrictions on config updates. It introduces no protocol fields or runtime dependencies.

## Current behavior

The current entry points do not enforce one consistent policy.

| Area | Current behavior | Relevant code |
| --- | --- | --- |
| HTTP config update | Rejects any target value of `keyspace_level`, including a valid repeated value; accepts unified, deletion, and unknown values | [HTTP keyspace handler](../../../server/apiv2/handlers/keyspace.go), `UpdateKeyspaceConfig` |
| Manager config update | Applies generic mutations without GC validation | [Keyspace Manager](../../../pkg/keyspace/keyspace.go), `UpdateKeyspaceConfig`, `UpdateKeyspaceConfigWithPreconditions`, `updateKeyspaceConfigTxn` |
| Public creation | Both kernel types accept explicit modes; NextGen replaces absent or empty values with `keyspace_level` | [Keyspace Manager](../../../pkg/keyspace/keyspace.go), `CreateKeyspace`, `CreateKeyspaceByID` |
| Reserved keyspaces | Bootstrap bypasses public creation and calls `saveNewKeyspace`; preallocation uses public creation | [Keyspace Manager](../../../pkg/keyspace/keyspace.go), `initReserveKeyspace`, `Bootstrap` |
| GC requests and enumeration | Interpret only the new field and do not recognize legacy-only v2 metadata | [GC state manager](../../../pkg/gc/gc_state_manager.go), `gcKeyspaceScope`, `iterateAllKeyspacesGCStates` |
| NextGen GC index | Stores the raw new field and loses the information needed to resolve legacy metadata | [Enabled-keyspace index](../../../pkg/gc/enabled_keyspace_index.go), `decodeKeyspaceMeta` |
| Client mode helper | Already implements the agreed precedence and legacy fallback | [Client helper](../../../client/keyspace_client.go), `IsKeyspaceUsingKeyspaceLevelGC` |

The [pd-ctl config command](../../../tools/pd-ctl/pdctl/command/keyspace_command.go) and the [Go HTTP client](../../../client/http/interface.go) call the same HTTP config endpoint. They must inherit the Manager policy without maintaining separate authoritative checks.

## Effective GC mode

All server decisions use one pure resolver in `pkg/keyspace`. The resolver does not validate requests, modify metadata, inspect the kernel, perform storage IO, or log. This separation preserves old data while rejecting invalid new inputs at the write boundary.

| Stored `gc_management_type` | Stored `safe_point_version` | Effective mode |
| --- | --- | --- |
| Present, `keyspace_level` | Any value or absent | Keyspace GC |
| Present, `unified` | Any value or absent | Unified GC |
| Present, empty or unknown | Any value or absent | Unified GC; report malformed stored config |
| Absent | `v2` | Keyspace GC |
| Absent | Any other value or absent | Unified GC |

Presence must be checked with a map lookup. An empty string counts as present and suppresses the legacy fallback. A nil config resolves to unified, independently of the kernel. Kernel defaults apply only during creation.

An illustrative server interface is `EffectiveGCManagementType(config map[string]string) string`, returning one of the two canonical constants. Exact helper names are left to the implementer. The existing client helper retains its API and behavior because it belongs to a separate Go module; tests must keep its truth table aligned with the server resolver.

## Approaches

The selected approach enforces writes in Manager and shares the resolver across GC readers. This covers direct Manager callers as well as HTTP and pd-ctl, while keeping storage responsible for persistence.

| Approach | Consequence | Decision |
| --- | --- | --- |
| Manager validation plus a shared server resolver | Covers existing production creation and config-update paths, preserves transactional checks, and supports legacy reads without rewriting them | Selected during the design discussion |
| HTTP and pd-ctl checks | Leaves direct Manager calls able to persist prohibited changes and duplicates policy across callers | Rejected because it cannot enforce the agreed invariant |
| Rewrite legacy data during reads or startup | Adds metadata writes and potential GC scope changes to reads or startup; expands this work into migration | Rejected because existing data must remain usable without automatic migration |

## Creation

Creation validates the original input before applying defaults so that an absent field can be distinguished from an explicitly empty field.

The shared creation policy performs these steps:

1. Reject the presence of `safe_point_version`, regardless of its value.
2. If `gc_management_type` is absent, insert the kernel's default.
3. Reject a present value other than `unified` or `keyspace_level`.
4. Reject `unified` under NextGen or `keyspace_level` under Classic.
5. Persist the explicit canonical value with the new metadata.

Both public creation methods must run GC validation before ID allocation, group assignment, or region-split side effects. Preserve the existing name and ID validation behavior. A shared check at the new-metadata persistence boundary also covers reserved-keyspace creation; public entry points still validate early to avoid side effects for rejected input.

Bootstrap must write explicit `unified` for a newly created Classic default keyspace and explicit `keyspace_level` for a newly created NextGen system keyspace. Existing reserved metadata must retain the existing `ErrKeyspaceExists` behavior and must not be rewritten on restart. Preallocated keyspaces inherit the public creation rules.

## Config updates

The Manager update transaction owns the complete validation and normalization sequence. Both update methods use the same implementation, including calls that do not come through HTTP.

The transaction performs the following sequence before any meta-service or TSO group reassignment:

1. Load the persisted metadata and enforce the existing allowed-state check.
2. Evaluate preconditions against the original stored config. In particular, a requirement that `gc_management_type` be absent must see the real stored absence.
3. Resolve and retain the old effective mode using the compatible resolver.
4. Reject any mutation targeting `safe_point_version`. Reject any supplied `gc_management_type` put value outside the two valid constants. Validate the requested mutations, rather than validating all untouched historical fields.
5. Apply the requested mutations to the candidate config.
6. Resolve the candidate's effective mode and reject the request if it differs from the old effective mode.
7. If the candidate lacks `gc_management_type`, insert the candidate's canonical effective mode.
8. Continue the existing group validation and save the complete metadata in the same transaction.

A failed precondition, policy check, group check, or transaction must not persist normalization or any part of the requested config update. Preconditions and existing transaction conflicts retain their current semantics. An empty successful config update also normalizes a missing new field; this does not happen on reads or keyspace-state updates.

The update path must not reuse the creation kernel check. For example, a legacy-only v2 keyspace under Classic can receive an unrelated config update and be normalized to explicit `keyspace_level`, even though creating that mode under Classic is prohibited. It keeps its effective mode and generates a diagnostic error for the kernel mismatch.

### Update examples

These examples define the edge cases an implementation must preserve. Omitted fields remain unchanged unless normalization adds the new GC field.

| Stored GC config | Requested config change | Result |
| --- | --- | --- |
| Neither GC field exists | Change an unrelated entry | Allow; persist `gc_management_type=unified`, including for a historical NextGen keyspace |
| Only `safe_point_version=v2` | Change an unrelated entry | Allow; retain the old field and add `gc_management_type=keyspace_level` |
| Only `safe_point_version=v2` | Set `gc_management_type=keyspace_level` | Allow; make the existing mode explicit |
| Only `safe_point_version=v2` | Set `gc_management_type=unified` | Reject the mode change |
| `gc_management_type=keyspace_level` | Repeat `keyspace_level` | Allow |
| `gc_management_type=unified` | Set `keyspace_level` | Reject the mode change |
| `gc_management_type=keyspace_level` | Set `unified` | Reject the mode change |
| `gc_management_type=keyspace_level`, no legacy v2 | Delete the new field | Reject because the resulting effective mode becomes unified |
| Both fields indicate keyspace GC | Delete the new field | Allow; normalize it back to `keyspace_level` before saving |
| New field is unified, legacy field is v2 | Delete the new field | Reject because the legacy fallback would activate keyspace GC |
| New field is unified, no legacy v2 | Delete the new field | Allow; normalize it back to `unified` before saving |
| Any stored config | Set or delete `safe_point_version`, including a no-op | Reject |
| Any stored config | Set the new field to an empty or unknown value | Reject |
| New field is empty or unknown | Change an unrelated entry | Allow; preserve the malformed value and report it through diagnostics |
| New field is empty or unknown | Set `gc_management_type=unified` | Allow; repair the representation without changing the effective mode |

## GC readers and index consistency

The compatible resolver must be applied at every source of a GC mode decision. Replacing the HTTP validation or request router alone is insufficient.

- `gcKeyspaceScope` must use the resolver. It is shared by ordinary redirection and the metadata-revision-aware transaction path.
- `iterateAllKeyspacesGCStates` must use the resolver before applying its cache predicate. Preserve the existing defense that emits unified markers before accepting cached per-keyspace state.
- `enabledKeyspaceIndex.decodeKeyspaceMeta` must derive a canonical effective mode from the complete metadata. Its entries must describe effective mode, rather than copying the raw new field. Both initial loading and watch updates pass through this decoder.
- Warmup, warmup skipping, and indexed watch enumeration must consume that effective index value. Audit their comparisons when changing the entry representation.

Preserve the transaction path's original metadata ID and revision even when the effective scope is NullKeyspace. A metadata update that only adds the explicit GC field still changes its revision, so an in-flight operation using the previous revision must retry or fail through the existing transaction-conflict behavior.

No new cache migration is required for successful normalization because it preserves the resolved scope. Preserve current cache-reset and stale-state defenses for leadership changes and external metadata changes.

## Diagnostics for existing metadata

Diagnostics run when existing code successfully reads or decodes persisted metadata. They must not change the metadata, return a policy error to the caller, or add storage requests.

Report an error when the effective mode does not match the kernel, or when the stored new field is explicitly empty or unknown. Missing new fields are valid legacy representations and do not themselves constitute an error. NullKeyspace is excluded. Observe metadata before filtering by keyspace state so that a visited disabled or archived keyspace can also be reported.

Coverage includes Manager loads by name, ID, ID with revision, and range; existing Manager reads during config updates, state updates, removals, group scans, and assignment patrols; and metadata decoded by the existing GC index loader or watcher. Reuse one Manager-owned diagnostic observer across these paths. The observer must receive the complete metadata, rather than the reduced enabled-index entry. State updates and other maintenance paths only observe GC config; they do not apply creation restrictions or normalize the fields.

Use structured error logs containing the keyspace ID, keyspace name, kernel, effective mode, presence and value of the new field, presence and value of the legacy field, and the violation reason. Do not log the entire config.

Deduplication is process-local and concurrency-safe. The proposed implementation uses a bounded cache owned by Manager, keyed by keyspace ID with a fingerprint of GC-relevant fields and the kernel. Repeated reads of the same violation are suppressed while the entry remains cached; a changed violation can be logged again. A valid observation clears its entry. Eviction or a new process can cause another log. Unrelated config changes must not change the fingerprint. The cache bound and concrete cache implementation are implementer choices and must be documented in the change.

The observer does not start a goroutine, timer, scan, or watcher. In particular, it must not enable the NextGen GC index under Classic solely for logging. Existing metadata that is never visited need not produce a diagnostic immediately.

## HTTP, clients, and errors

The service exposes Manager policy consistently through the existing endpoints. pd-ctl remains a caller of the service and uses the service result as the authority.

Remove the handler's unconditional rejection of the target `keyspace_level` value. Preserve the unrelated prohibition on changing `region_bound_type`. Apply the common policy-error mapping to both creation endpoints and config updates.

The design proposes distinguishable repository-style errors for invalid GC input, kernel-incompatible creation, a prohibited effective-mode change, and use of the legacy field in a write request. Map these policy errors to HTTP 400, retain HTTP 409 for existing precondition and transaction conflicts, and retain HTTP 500 for storage or internal failures. Exact error identifiers and messages are implementer choices. Regenerate `errors.toml` if normalized errors are added, and update handler annotations for the resulting responses.

Keep both HTTP client update methods and the exported client mode helper compatible at the API level. Correct the HTTP client's stale comments that name `global_gc` and `keyspace_level_gc`; the recognized values are `unified` and `keyspace_level`. Add pd-ctl regression coverage for repeated valid values, both prohibited conversions, creation rules, and rejection of legacy-field writes. Client-side checks are optional conveniences and cannot replace Manager validation.

## Legacy storage and deployment behavior

Compatibility changes the current server interpretation of metadata that has no new field and has `safe_point_version=v2`. Those keyspaces use their per-keyspace scope under this design, matching the existing client helper. This is a read-semantics correction and must be described explicitly in the eventual change description; preserving stored metadata does not mean that this particular current-server behavior remains unchanged.

The old v2 safe-point and local-barrier paths already match the current per-keyspace paths, so existing values are reused. No unified safe point, transaction safe point, or barrier is copied, merged, or used to initialize those paths. If per-keyspace state is absent, the existing storage behavior returns zero safe points and an empty local-barrier list. This work does not introduce a recovery or state-migration procedure.

Policy checks apply to requests handled by the updated binary. Older PD members do not acquire the new constraints during a mixed-version deployment. This design does not add a cluster-version gate or change leader election to make the policy cluster-wide before all serving members are updated.

## Validation

Validation must prove behavior at the policy boundary and at real request-routing boundaries. Test fixtures that require unsupported historical metadata must construct that metadata through test-only storage setup; production validation must not gain a bypass switch.

| Test area | Required coverage |
| --- | --- |
| Resolver | Full precedence table, nil config, presence versus absence, empty and unknown values, and agreement with the existing client helper |
| Creation | Both kernel types; both public methods; absent, valid, empty, unknown, incompatible, and legacy-field inputs; reserved-keyspace defaults; preallocation; no side effects for policy rejection |
| Config updates | Both directions rejected; repeated valid value allowed; legacy writes always rejected; automatic normalization; deletion examples above; unchanged malformed history allowed; explicit malformed writes rejected; mismatch updates allowed |
| Atomicity | Preconditions see original metadata; failures do not normalize or partially persist; policy failure happens before group side effects; existing metadata-revision conflict handling survives normalization |
| Diagnostics | Kernel mismatch and malformed-value errors; no error for a valid matching legacy representation; repeated and concurrent observations deduplicated; changed and repaired observations handled; reads succeed and do not write |
| GC integration | Legacy-only v2 uses per-keyspace safe points and barriers through ordinary reads, transaction paths, full enumeration, initial index load, watch updates, warmup, and cached reads |
| Storage compatibility | Existing v2 state is reused; absent per-keyspace state retains existing zero/empty behavior; unified state is neither modified nor copied |
| HTTP and pd-ctl | Both creation routes and config update enforce the same policy; HTTP status mapping; valid idempotent keyspace-level updates reach Manager; legacy put/delete requests fail |
| Restart and history | Existing incompatible reserved metadata is retained; historical configurations can still be read and receive unrelated updates; restarts do not rewrite GC config |

Existing GC tests create mixed modes under Classic, and some tests intentionally convert modes to exercise stale-cache defenses. Preserve that behavioral coverage by setting up historical or externally changed metadata directly in test storage. Rewrite tests that currently assert successful unknown GC writes or successful cross-kernel creation. Keep tests of the production validation separate from historical-data fixture setup.

Run the relevant package suites under Classic and NextGen, then the HTTP, client-integration, and pd-ctl suites in their Go modules. Use failpoint-aware make targets. For example, the root package commands are:

```sh
make gotest GOTEST_ARGS='-tags=without_dashboard ./pkg/keyspace ./pkg/gc -count=1'
NEXT_GEN=1 make gotest GOTEST_ARGS='-tags=nextgen,without_dashboard ./pkg/keyspace ./pkg/gc -count=1'
```

The root `gotest` target passes `GOTEST_ARGS` directly to `go test`, so the NextGen command includes the tag explicitly. Use the appropriate module's `gotest` target for client, integration, and tools packages. Run the required static and formatting checks before submission, including `make check`, and leave failpoints disabled. Dependency changes are not expected.

## Implementation handoff

Prepare the implementation plan and delegate implementation to a sub-agent. The handoff must include this approved document and the verification requirements.

The implementation naturally separates into these responsibilities; the later plan determines agent ownership and sequencing:

1. Add the server resolver, creation policy, update invariants, normalization, and diagnostic observer in `pkg/keyspace`, with focused tests.
2. Integrate the resolver and observer into GC routing, enumeration, and the enabled-keyspace index while preserving revision and cache safeguards.
3. Update HTTP error mapping and comments, and add service, HTTP-client, and pd-ctl regression coverage.
4. Adapt historical-data fixtures, run both kernel configurations, and review the completed implementation against this specification.

Exact helper names, internal file placement, diagnostic cache size, normalized error names, and test-only fixture helpers are implementer choices. The resolver truth table, creation policy, update ordering, legacy-field rejection, grandfathering, and absence of automatic GC-state migration are fixed requirements.
