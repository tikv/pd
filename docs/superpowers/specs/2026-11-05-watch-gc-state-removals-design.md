# WatchGCStates cache removal

Send best-effort `removed` events when keyspaces leave `ENABLED`, so CSE can release their GC cache entries. The user approved this scope and delegated implementation on November 5, 2026.

## Behavior

Reuse the GC leader's existing enabled-keyspace metadata watch. When a progress batch removes an ID that was in the enabled index, report that ID through a persistent removal callback. Invoke it outside the index mutex. The GC manager checks the callback's leadership generation under its existing mutex and publishes `NewGCStateRemoved(id)` through the existing watcher queue.

Only an actual exit from the enabled index produces a removal. Later non-enabled metadata updates do not repeat it. If the final coalesced state is enabled, no removal is needed. The callback remains installed after initial warmup hooks are cleared. Initial snapshots and recovery reloads do not synthesize removals.

Occasional missing notifications and later upserts that reinsert removed entries are acceptable. Reconnection already resets the CSE cache. Keep existing queue limits and slow-consumer behavior. Do not add tombstones, publication filters, reload diffs, retries, forced reconnects, new protocol fields, CSE changes, or GC-mode migration support.

## Approach

The existing metadata watch observes committed state and already belongs to the GC leadership generation. Reuse it rather than adding notifications to each metadata mutation API. Strong lifecycle synchronization and reload reconciliation were considered and rejected because this feature is a memory optimization with explicitly best-effort delivery.

## Validation

Verify real metadata changes reach a GC watcher; repeated non-enabled changes do not repeat removal; coalesced enablement avoids removal; callbacks run outside the index mutex; retired generations cannot notify new streams; and bulk removal follows existing queue behavior. Preserve ordinary upsert behavior and existing watcher initialization tests. Run failpoint-aware GC package tests in classic and NextGen modes, focused race checks, formatting, and relevant lint checks.
