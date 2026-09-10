# Storage

This guide covers PD metadata storage in `pkg/storage/`.

## Purpose And Scope

Storage owns:

- the composite `Storage` interface
- etcd-backed metadata storage
- memory storage for tests
- local LevelDB-backed region metadata storage
- endpoint-specific metadata contracts
- region storage switching and one-time region loading behavior
- hot-region storage

Storage does not decide scheduling, leadership, or API semantics. It provides
the persistence contracts used by those subsystems.

## Core Concepts

- Most PD metadata is stored through etcd-backed storage and endpoint-specific
  interfaces.
- Region metadata can be routed through `coreStorage` to a local LevelDB-backed
  region storage.
- Key paths under `pkg/utils/keypath` are compatibility contracts, not local
  implementation details.
- `TryLoadRegionsOnce` prevents repeated full local-region loads after the
  local backend has been selected.
- Hot-region storage is a separate local persistence surface for historical hot
  region data.

## Architectural Views

### Backend view

Most metadata uses an etcd-backed `Storage`. Tests can use memory storage. Region
metadata can be stored in a specialized local LevelDB-backed `RegionStorage`
through `coreStorage`.

### Endpoint view

`pkg/storage/endpoint` splits metadata contracts by owner: config, meta,
placement rules, replication status, GC state, min resolved TS, external TS,
keyspace, resource group, TSO, keyspace group, meta-service group, maintenance,
and affinity.

### Region storage view

`NewCoreStorage` wraps a default storage and region storage. Region load/save
calls route to etcd or local region storage depending on
`TrySwitchRegionStorage`. `TryLoadRegionsOnce` prevents repeated full loads from
local region storage.

## Process Lifecycle And Startup Sequencing

Important anchors:

1. `storage.NewStorageWithEtcdBackend`
2. `storage.NewRegionStorageWithLevelDBBackend`
3. `storage.NewCoreStorage`
4. `storage.TrySwitchRegionStorage`
5. `storage.TryLoadRegionsOnce`
6. `Storage.Close`

`server.startServer` creates the default etcd storage, the region LevelDB
storage, then wraps them in `coreStorage`. `RaftCluster` loads cluster metadata
from storage during leader startup and persists updates during heartbeat
processing and background jobs.

Maintenance rules:

- Region storage routing must be explicit. Do not assume every metadata path
  uses etcd.
- Local region storage load state is intentionally tracked to avoid repeated
  full loads.
- Storage close currently closes the region storage in `coreStorage`.

## Data Model And Metadata Contracts

Hot contracts:

- key paths under `pkg/utils/keypath`
- endpoint interfaces in `pkg/storage/endpoint`
- region metadata load/save/delete behavior
- persisted scheduler and placement config
- TSO and keyspace group metadata
- GC state, external TS, and min resolved TS
- service middleware and maintenance metadata

Maintenance rule:

- If a metadata key changes, review all readers, writers, migrations, clients,
  and downgrade behavior.

## Observability And Operational Signals

Open these first:

- `pkg/storage/kv/metrics.go`
- `pkg/storage/hot_region_storage.go`
- `pkg/storage/region_storage.go`
- `server/cluster/metrics.go`

Signals to preserve:

- storage operation metrics
- logs for failed region save/delete
- hot-region storage lifecycle signals
- cluster warm-up duration and load errors

## Change Management Guidance

- New metadata should be added through endpoint interfaces, not ad hoc key
  access from unrelated packages.
- Persistence changes need tests against etcd/memory behavior when practical.
- Region storage changes need load-once, switch, flush, and close-path tests.
- Any persisted format change needs compatibility review.

## Must-Read File Order

1. `pkg/storage/storage.go`
2. `pkg/storage/etcd_backend.go`
3. `pkg/storage/memory_backend.go`
4. `pkg/storage/leveldb_backend.go`
5. `pkg/storage/region_storage.go`
6. `pkg/storage/hot_region_storage.go`
7. `pkg/storage/kv/kv.go`
8. `pkg/storage/kv/etcd_kv.go`
9. `pkg/storage/endpoint/endpoint.go`
10. `pkg/utils/keypath`

## Glossary

- Default storage:
  usually the etcd-backed `Storage` implementation used for most metadata.
- Endpoint storage:
  owner-specific storage interface under `pkg/storage/endpoint`.
- `coreStorage`:
  wrapper that routes region metadata to etcd or local region storage.
- Region storage:
  storage interface for loading, saving, deleting, and flushing region metadata.
- Local region storage:
  LevelDB-backed region metadata store under the PD data directory.
- Key path:
  etcd key layout helper that acts as a persisted compatibility boundary.
- Hot-region storage:
  local store for historical hot region records used by API and diagnostics.

## Review Checklist

- Is the metadata persisted under the right ownership prefix?
- Are readers and writers using the same endpoint contract?
- Does the change work with both memory and etcd-backed tests?
- Does region metadata route correctly when local region storage is enabled?
- Are load, save, delete, flush, and close semantics preserved?
- Does the change need migration, cleanup, or compatibility handling?
- Are storage errors propagated or intentionally logged-and-continued?
