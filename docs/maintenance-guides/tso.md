# TSO

This guide covers the timestamp oracle in `pkg/tso/` and the TSO microservice
surface under `pkg/mcs/tso/`.

## Purpose And Scope

TSO owns:

- timestamp allocation
- timestamp physical/logical update windows
- allocator initialization and reset
- PD leader or TSO primary ownership checks
- keyspace group TSO routing
- TSO request handling in embedded and microservice modes
- TSO metrics and failure signals

TSO does not own transaction semantics in TiDB or storage MVCC in TiKV, but
those systems depend on TSO monotonicity and availability.

## Core Concepts

- TSO correctness is monotonicity across leader changes, service-primary changes,
  process restarts, and retries.
- `Allocator` serves timestamps only after ownership and initialization are both
  valid.
- `timestampOracle` maintains the in-memory timestamp and persisted upper-bound
  window.
- Keyspace group mode routes TSO requests by keyspace group ownership, not only
  by PD leadership.
- Reset and logical overflow paths are correctness-sensitive because they change
  the timestamp window.

## Architectural Views

### Embedded PD view

Classic PD creates a `tso.Allocator` in `server.startServer`. When the PD member
becomes leader, `RaftCluster` initializes the allocator and `GrpcServer.Tso`
serves requests from it.

### Microservice view

The TSO service in `pkg/mcs/tso/server` runs primary election independently and
uses keyspace group metadata to decide which allocator serves a request.
Classic PD can fall back to embedded TSO depending on keyspace group mode,
service discovery, and dynamic switching config.

### Timestamp oracle view

`timestampOracle` owns the persisted timestamp window and in-memory TSO object.
The allocator updater periodically advances the window while serving.

## Process Lifecycle And Startup Sequencing

Important anchors:

1. `tso.NewAllocator`
2. `Allocator.allocatorUpdater`
3. `Allocator.Initialize`
4. `Allocator.UpdateTSO`
5. `Allocator.GenerateTSO`
6. `Allocator.Reset`
7. `Allocator.primaryElectionLoop`
8. `pkg/tso/keyspace_group_manager.go`
9. `pkg/mcs/tso/server/server.go`
10. `pkg/mcs/tso/server/grpc_service.go`

Maintenance rules:

- Serving requires both ownership and allocator initialization.
- Reset must clear the allocator role and timestamp state before leadership is
  released or retried.
- Physical time updates are correctness-sensitive and should not block on
  unrelated work.

## Data Model And Metadata Contracts

Hot contracts:

- physical and logical timestamp components
- TSO save interval and update physical interval
- max reset TS gap
- keyspace group ID
- expected primary flags in microservice mode
- TSO service discovery entries
- persisted timestamp upper bound

Maintenance rule:

- TSO must be monotonic across leader changes, primary changes, process restarts,
  and network retries.

## Observability And Operational Signals

Open these first:

- `pkg/tso/metrics.go`
- `pkg/mcs/tso/server/metrics.go`
- `server/grpc_service.go`
- `server/metrics.go`

Signals to preserve:

- allocator role gauge
- not-leader and forwarding failures
- TSO update errors and reset logs
- proxy stream timeout behavior
- microservice primary election logs

## Change Management Guidance

- TSO behavior changes need tests for leader transfer, reset, and logical
  overflow when applicable.
- Microservice changes need both embedded fallback and independent primary
  review.
- Request boundary changes need client and gRPC compatibility review.
- Config changes need persisted config and runtime side-effect review.

## Must-Read File Order

1. `pkg/tso/allocator.go`
2. `pkg/tso/tso.go`
3. `pkg/tso/keyspace_group_manager.go`
4. `pkg/tso/config.go`
5. `server/grpc_service.go`
6. `server/cluster/cluster.go`
7. `pkg/keyspace/tso_keyspace_group.go`
8. `pkg/mcs/tso/server/server.go`
9. `pkg/mcs/tso/server/grpc_service.go`
10. `client/client.go`

## Glossary

- TSO:
  timestamp oracle used by TiDB transaction ordering.
- Allocator:
  component that initializes timestamp state and generates timestamps.
- `timestampOracle`:
  lower-level owner of persisted timestamp window and in-memory TSO state.
- Physical time:
  wall-clock component of a timestamp.
- Logical time:
  counter component used when multiple timestamps share the same physical time.
- TSO primary:
  elected service owner allowed to serve TSO in microservice mode.
- Keyspace group:
  group of keyspaces assigned to a TSO ownership unit.

## Review Checklist

- Can this member serve TSO only in the correct leader or primary state?
- Is timestamp monotonicity preserved across retries and resets?
- Are persisted timestamp windows advanced before serving new timestamps?
- Does the change affect keyspace group routing?
- Does dynamic switching between embedded PD and TSO service remain safe?
- Are client timeout, forwarding, and stream-close semantics unchanged?
- Do tests use failpoint-aware make targets when failpoints are involved?
