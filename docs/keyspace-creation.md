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

## Retrying a failed create

The caller decides whether to retry. A Create request for an existing
`DISABLED` keyspace with a pending token restores the caches and continues
creation using the stored ID and configuration. A supplied ID must match that
object. Retry configuration does not replace the stored configuration; use the
separate config API to change it. A non-pending object, including an already
`ENABLED` keyspace, retains the existing `ErrKeyspaceExists` behavior. After an
uncertain enable response, query the keyspace to confirm whether it is enabled.

The enable transaction rechecks the original creation token and state, then
atomically sets `ENABLED` and removes the token. This prevents an old request
from enabling a new object that reused the original ID after deletion.

## Management operations

- Users cannot set or delete `creation_pending` through Create or config APIs.
- A manual enable of a disabled pending keyspace is rejected while the current
  server setting requires waiting. Retry Create to finish it instead.
- With waiting disabled in the current server setting, manual enable is allowed
  and removes the token atomically.
- Reapplying `DISABLED` remains a no-op; it does not cancel pending creation.
- Archiving cancels creation and removes the token in the same transaction.
  Deletion and recreation retain the existing lifecycle rules.
- Old disabled objects without a token are not adopted as pending creations.

These recovery semantics require a PD version that understands the token. An
older leader does not resume pending creates and does not enforce the new manual
enable guard. Enable this workflow only after completing the PD upgrade when
those guarantees must hold throughout the rollout.
