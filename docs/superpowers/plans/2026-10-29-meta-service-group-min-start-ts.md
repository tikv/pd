# Meta-service group min-start-ts implementation plan

> **For agentic workers:** Use `superpowers:subagent-driven-development` to execute this plan. The user has authorized writing the design and immediately handing implementation to a sub-agent.

**Goal:** Honor TiDB transaction protection stored in an assigned meta-service group's etcd.

**Architecture:** Extend the existing group manager with lazy clients, reuse redirect metadata to select the report backend, and reuse the current min-start-ts parser. All PD GC state writes remain on PD's backend.

**Tech stack:** Go, etcd v3, existing PD storage and embedded-etcd test helpers.

**Spec:** [Design](../specs/2026-10-29-meta-service-group-min-start-ts-design.md).

## Global constraints

The design is authoritative. Keep the change focused on issue #11437.

- Migration, dual reads, dual writes, and rolling-upgrade compatibility are outside this change.
- All PD GC state writes remain on PD's backend; any group lookup/read/parse error aborts advancement.
- Reuse report parsing and already loaded keyspace metadata. Cache clients by group, never reports by keyspace.
- Use the current model tier and reasoning effort for every sub-agent.
- Follow repository `AGENTS.md`; use Go 1.27.3 via `GOTOOLCHAIN=auto`; disable failpoints immediately after each test command, before editing or other work.
- Add no runtime dependencies or generic routing framework. Markdown prose is not hard-wrapped. Commit with sign-off and subjects at most 70 characters, bodies wrapped at 72.

## Review focus

The task's tests must cover these cross-cutting failure modes in addition to the direct bug.

- Both group configuration update paths refresh cached clients.
- Disabled groups remain readable for existing keyspaces.
- A configured group with missing infrastructure fails instead of silently using PD.
- The legacy `gc_worker` path uses the same routing decision.
- Shutdown and concurrent first access do not leak or recreate owned clients.

### Task 1: Route min-start-ts reads through managed group clients

This is one cohesive implementation task: client ownership, GC routing, and server wiring must work together for the regression to pass. Read the linked design before implementing.

**Files:** Create `pkg/keyspace/meta_service_group_client.go`, `pkg/keyspace/meta_service_group_client_test.go`, and `pkg/gc/tidb_min_start_ts.go`; modify `pkg/keyspace/meta_service_group.go`, `pkg/gc/gc_state_manager.go`, `pkg/gc/gc_state_manager_test.go`, and `server/server.go`. Update other constructor call sites and test helpers as required. Use a dedicated `pkg/gc/tidb_min_start_ts_test.go` if that keeps the new integration fixture clearer.

**Interfaces:** Add `(*MetaServiceGroupManager).GetEtcdClient(groupID string) (*clientv3.Client, error)` and `Close() error`. Extend `NewGCStateManager` with a fourth `*keyspace.MetaServiceGroupManager` argument. Choose private helper signatures consistent with the design; no new public reader interface is required.

- [ ] Establish a narrow baseline with existing group and min-start-ts tests. The initial shell has Go 1.25; use `GOTOOLCHAIN=auto`. Existing pinned tools are linked into `.tools/bin`; `make -o install-tools gotest` can reuse them without reinstalling. Sandbox cache/socket failures require escalation, not product changes.
- [ ] Add the two-etcd leased-report regression with report 100 and target 200. Configure the actual keyspace/group manager fixture, keeping report and PD state storage separate. Before production changes, run it and capture the expected incorrect result of 200 or missing protection.
- [ ] Implement lazy client ownership, endpoint reconciliation in both configuration paths, deletion cleanup, idempotent close, and associated client tests. Maintain existing per-endpoint health validation.
- [ ] Implement routing and metadata reuse for both advancement entry points. Preserve the existing `redirectKeyspace` wrapper for unrelated call sites. Reuse `CompatibleLoadTiDBMinStartTS` on a temporary provider around the cached group client; keep GC writes on the original provider.
- [ ] Wire the group manager into production startup and shutdown and all constructor callers. Extend tests for the design's isolation, failure, lease-removal, local fallback, barrier, and legacy API cases.
- [ ] Run the regression and affected packages with failpoints enabled automatically: `GOTOOLCHAIN=auto make -o install-tools gotest GOTEST_ARGS='./pkg/gc ./pkg/keyspace ./pkg/storage/endpoint -count=1'`, then the same packages with `-tags nextgen`. Run targeted `-race` tests for the new client lifecycle tests and routing fixture. Use `make basic-test` restricted to touched packages if needed to satisfy repository checks without testing unrelated integration packages.
- [ ] Format touched Go files, run applicable static checks and a server compile check, inspect the final diff and module files, and confirm failpoints are disabled. Record exact commands, failures, and limitations in the implementation report. Avoid duplicate verification after a passing check unless code changes or a new concern requires it.
- [ ] Commit the implementation with sign-off. Report the commit, RED/GREEN evidence, test results, and any concerns; the parent agent performs the independent review. Do not push or create a PR.
