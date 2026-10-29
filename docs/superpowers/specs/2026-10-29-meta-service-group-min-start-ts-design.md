# Route TiDB min-start-ts reads to the assigned meta-service group

Fix [PD #11437](https://github.com/tikv/pd/issues/11437): when TiDB reports min-start-ts to a dedicated meta-service group's etcd, PD must read that backend before advancing the keyspace's transaction safe point. The feature has not shipped; migration, dual reads, dual writes, and rolling-upgrade compatibility are outside this change.

## Existing behavior and chosen approach

`GCStateManager.advanceTxnSafePointImpl` reads min-start-ts through `gcMetaStorage`, which always uses PD's etcd. An empty prefix is interpreted as no protection. `MetaServiceGroupManager` already owns group endpoints and a TLS loader, but its only external clients are temporary health-check clients. The storage layer already implements the required range read and timestamp parsing.

Extend the group manager with lazy, shared etcd clients and route only the min-start-ts read. Reuse the existing storage reader through a lightweight provider wrapping the selected client. This avoids a second parser, a general routing framework, and changes to the GC state transaction protocol. Creating a client per read would add connection and TLS setup to each advancement; moving TiDB to barrier reporting would require a separate cross-component protocol change.

## Client ownership

Add `GetEtcdClient(groupID string) (*clientv3.Client, error)` and `Close() error` to `MetaServiceGroupManager`, with the implementation in `pkg/keyspace/meta_service_group_client.go`.

- Cache one client per group, protected by the manager's existing mutex. Create clients lazily from the current endpoints and TLS loader; failed construction must not populate the cache. Use existing etcd client construction conventions and bounded dial timeouts.
- The manager owns returned clients. Callers borrow them and must not close them or change their endpoints. The existing health check remains independent because it checks each endpoint before publishing configuration.
- Both `persistGroupsLocked` and `updateGroups` must reconcile cached clients when applying configuration. Update an existing client's endpoints for an address change, close and remove clients for deleted groups, and retain clients for unchanged groups. An endpoint edit refers to the same etcd cluster; online data migration is outside scope.
- A disabled group can still serve assigned keyspaces. `Enabled` controls new assignment, not whether existing transaction protection is readable.
- Reject unknown groups, empty or malformed endpoint lists, and use after close. `Close` must be idempotent, close every owned client, and prevent later client creation. Check or report close errors using existing project conventions.
- Reuse the existing mutex and standard client concurrency guarantees. No reference-counted leases, background eviction, periodic probes, or new metrics are needed.

## GC integration

Pass the group manager explicitly to `NewGCStateManager` as a fourth argument. Update its production and test call sites; tests without groups pass nil. Server shutdown closes the group manager after stopping server loops.

Reuse the keyspace metadata already loaded during redirect/validation. Add an internal redirect variant returning the metadata as well as the existing ID and diagnostic name; retain the original wrapper for unrelated GC operations. Only `AdvanceTxnSafePoint` and the `gc_worker` path in `CompatibleUpdateServiceGCSafePoint` need to pass that metadata into `advanceTxnSafePointImpl`. This avoids a second metadata read on each advancement.

Add a private min-start-ts loading helper to `pkg/gc/tidb_min_start_ts.go`. Its backend selection is:

| Input | Backend or result |
| --- | --- |
| Redirected NullKeyspace | Existing PD provider |
| Keyspace without `meta_service_group_id` | Existing PD provider |
| Keyspace with a valid group ID | Client from `MetaServiceGroupManager.GetEtcdClient` |
| Present but invalid group ID, unknown group, or unavailable manager for a configured group | Error |

For a group client, wrap `kv.NewEtcdKVBase(client)` in `endpoint.NewStorageEndpoint(..., nil)` and call its existing `GCStateProvider.CompatibleLoadTiDBMinStartTS`. These small wrappers borrow the cached client and do not own or close it. Keep using the existing full keyspace prefix exactly once, the existing uint64 parser, and the existing bounded `etcdutil` range read. Do not cache min-start-ts results.

The selected provider is local to this read. `m.gcMetaStorage`, safe points, barriers, global barriers, and the GC revision key stay on PD's backend. Keep the report read inside the existing GC state transaction callback, so any routing, TLS, read, or parsing error aborts the batch. Successful empty reads retain existing semantics; failed reads never fall back to PD or become empty success. This preserves the existing protection semantics without claiming an atomic transaction across etcd clusters.

## Correctness and cost

For target 200, current transaction safe point at most 100, and a live report of 100 in the assigned group, advancement must stop at 100 and identify the reporting TiDB. If the current safe point already exceeds a report, preserve the existing monotonic behavior. Existing barriers still participate in selecting the minimum blocker.

The steady-state change adds a client-cache lookup and reads the correct backend instead of PD's backend; it does not add a second report scan or metadata lookup. Client count scales with accessed groups rather than keyspaces. External reads retain a finite timeout because advancement currently holds the GC manager mutex.

## Verification

Use existing embedded-etcd helpers and failpoint-aware test commands. Add the regression before the implementation and capture its failure on the old behavior.

1. Two independent etcd clusters: PD A and group B. Store a leased report of 100 only in B, advance to 200, and assert the returned and persisted safe point is 100 with the expected owner. Confirm the safe point and GC revision remain on A and are absent from B. Revoke the report lease, confirm removal, and assert a subsequent advancement reaches 200.
2. Routing isolation: another keyspace's lower report must not interfere; a conflicting report in A must not substitute for the assigned group's report. Preserve NullKeyspace and no-group behavior.
3. Failure safety: missing group, invalid group configuration, TLS-loader error, unavailable backend, and malformed report must return errors without changing persisted safe points or committing staged GC writes. Use bounded or canceled test requests where possible instead of long sleeps.
4. Client behavior: concurrent lookups reuse a client; configuration updates reach the new endpoints; deletion and repeated close clean up clients; failed lookup or construction can recover after valid configuration is restored; a disabled assigned group remains readable.
5. Exercise the legacy `gc_worker` advancement path and composition with existing barriers. Run affected package tests in default and `nextgen` modes, targeted race tests for client ownership, formatting, and applicable static checks.

## Implementation boundary

Changes belong in `pkg/keyspace` for client ownership, `pkg/gc` for backend selection and tests, and `server/server.go` for wiring and shutdown. Existing endpoint parsing tests remain the source of truth for the key/value format. No dependency, public HTTP/gRPC API, or TiDB change is required.
