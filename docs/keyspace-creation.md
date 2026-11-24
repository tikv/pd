# Recoverable keyspace creation

Keyspace creation atomically persists the name/ID mapping, metadata, TSO group
membership, meta-service group assignment/count, and region label rule. An
ordinary new keyspace starts as `DISABLED` with a PD-owned `creation_pending`
config entry. The entry is a unique creation token, not a user setting.

After commit, PD refreshes its caches from storage. Cache synchronization retries
are bounded; an exhausted read failure, a missing region label rule, a split wait
timeout, or an enable failure returns an error without deleting the keyspace.
The HTTP API retains its existing error format and returns HTTP 500 for these
failures. An error does not imply that nothing was persisted.

When `wait-region-split` is enabled, the request waits synchronously for the
actual region boundaries before enabling the keyspace. A request reads this
setting at its start; a later request uses the then-current server setting.
When waiting is disabled, cache synchronization still finishes before enabling,
but the request does not wait for the split. Client readiness checks continue
to check the actual region boundaries. Reserved keyspace bootstrap retains its
non-waiting behavior and idempotent repair.

## Recovering a failed create

Create rejects an existing name or ID with `ErrKeyspaceExists`, including a
`DISABLED` keyspace with a pending token. It never adopts an existing object or
silently ignores conflicting settings in a duplicate request. After a failed
create, query the keyspace to determine whether it was persisted or enabled.
Use the state API to manually enable a persisted pending keyspace, and the
separate config API to change its settings. Retry Create only if the object
does not exist.

The enable transaction rechecks the original creation token and state, then
atomically sets `ENABLED` and removes the token. This prevents an old request
from enabling a new object that reused the original ID after deletion.

For the original Create's enable transaction only, a commit-stage deadline,
transport-unavailable error, or recognized etcd request timeout uses optimistic
`ENABLED` cache state when a fresh read still identifies the same disabled pending
generation. The API still returns the error. Read, validation, staging, transaction
conflict, and explicit etcd rejection errors do not publish optimistic state.
This is a deliberate trade-off similar to master's pre-commit publication: if
the uncertain transaction never commits, cache state can lead storage. Query
metadata and use manual enable to complete the durable transition. A failed
reconciliation read invalidates the cache instead of guessing identity.
Ordinary state updates, including manual ENABLE, retain their existing handling
of uncertain transaction results and are outside this change's scope.

## Management operations

- Users cannot set or delete `creation_pending` through Create or config APIs.
- Manual enable first restores the group and region label caches, regardless of
  the server's wait-region-split setting. A refresh failure returns
  an error and preserves the token. The enable transaction rechecks the creation
  identity and current state before removing the token atomically.
- Manual enable does not wait for region split. As on master, `ENABLED` does not
  mean split-ready; client readiness checks still require actual boundaries.
- Reapplying `DISABLED` remains a no-op; it does not cancel pending creation.
- Pending keyspaces cannot be archived, tombstoned, or removed. Complete manual
  enable first; ordinary disable/archive/removal and ID reuse are then allowed.
- Old disabled objects without a token are not adopted as pending creations.

These recovery semantics require a PD version that understands the token. An
older leader does not enforce pending archive/removal guards or restore creation
caches during manual enable. Use this workflow only after completing the PD upgrade when
those guarantees must hold throughout the rollout.
