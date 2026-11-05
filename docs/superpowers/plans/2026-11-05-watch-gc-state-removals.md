# WatchGCStates cache removal implementation plan

> **For agentic workers:** Use `superpowers:subagent-driven-development`. Steps use checkboxes for tracking.

**Goal:** Let CSE reclaim GC cache entries when keyspaces leave ENABLED.

**Architecture:** Add a persistent removal callback to the existing enabled index. Wire it to the GC manager's existing publisher with a leadership-generation check and no additional recovery mechanism.

**Tech stack:** Go 1.27.3, etcd client v3, existing Go failpoint tests.

**Spec:** [Approved design](../specs/2026-11-05-watch-gc-state-removals-design.md).

## Global constraints

- Delivery is best effort: occasional missing removals and later upserts are acceptable until reconnection resets the CSE cache.
- Emit removals only when an ID leaves the existing enabled index during a watch progress batch.
- Invoke callbacks outside the index mutex, and validate leadership generation under the manager mutex.
- Preserve existing queue limits and slow-consumer behavior.
- No tombstones, publication filters, reload diffs, retries, forced reconnects, protocol changes, CSE changes, or GC-mode migration support.
- Follow AGENTS.md, keep failpoints disabled outside test execution, and do not hard-wrap Markdown prose.

## Review focus

Check callback lifetime after initial hooks clear, lock ordering, late generation callbacks, coalesced transitions, and burst removal behavior. Each is covered by Task 1.

### Task 1: Publish best-effort removals from the enabled index

**Files:** Modify `pkg/gc/enabled_keyspace_index.go`, `pkg/gc/gc_state_lifecycle.go`, and `pkg/gc/gc_state_watcher.go`; add focused tests in their existing test files or a small focused removal test file in `pkg/gc`.

**Interfaces:** Add a persistent optional `onRemoved func([]uint32)` callback on `enabledKeyspaceIndex`. In `publishProgress`, collect IDs present before a nil entry removes them, finish the index update, unlock, and invoke the callback. Wire the callback before the generation's index worker starts. A manager helper may accept `(generation *gcStateGeneration, ids []uint32)`, acquire `m.mu`, validate `m.activeGeneration.Load() == generation`, and publish existing removal values. Keep the unrelated uncertain-commit TODO; narrow the lifecycle TODO to metadata upserts that remain deferred.

- [ ] Write a real metadata-watch-to-GC-watcher regression test using existing embedded-etcd helpers. An enabled keyspace becoming disabled must yield `Removed(id)`. Verify it fails because the notification is missing before implementing production changes.
- [ ] Implement the callback and generation-checked publication with no new worker, queue, or storage reads.
- [ ] Cover repeated disabled/archived updates, metadata deletion of an indexed keyspace, never-enabled IDs, coalesced final-enabled state, callback lifetime and ability to read the index without deadlock, old-generation callbacks, and a bulk batch using existing backpressure semantics. Keep tests focused on behavior; use deterministic synchronization and existing failpoint patterns.
- [ ] Run focused tests while iterating, then the full `pkg/gc` suite in classic and NextGen modes and focused race tests. Use `make gotest` or correctly bracket manual tests with failpoint enable/disable. Format touched Go files and run relevant lint checks. Record exact commands and results in the implementer report.
- [ ] Review the diff, ensure failpoints are disabled, and create a signed-off implementation commit with a subject such as `gc: notify watchers when keyspaces leave enabled state`. Commit only intended source and test files; the controller manages plan documents.
