# Keyspace And Microservices

This guide covers keyspace metadata in `pkg/keyspace/` and PD microservice
infrastructure in `pkg/mcs/`.

## Purpose And Scope

Keyspace owns:

- keyspace creation, lookup, state transitions, and config mutation
- reserved/default keyspace bootstrap
- keyspace region split coordination
- keyspace group assignment, split, merge, and keyspace movement
- meta-service group assignment

Microservices own:

- service registration and discovery
- primary election for split services
- TSO, scheduling, resource manager, router, and meta-storage servers
- forwarding and fallback behavior between PD and independent services

## Architectural Views

### Keyspace metadata view

`keyspace.Manager` validates requests, serializes mutations with keyed locks,
persists metadata through endpoint storage, updates local lookup caches, and
coordinates region-bound labels and group assignment.

### Keyspace group view

`keyspace.GroupManager` manages keyspace group metadata used by TSO routing. It
supports bootstrap, group creation, keyspace movement, split, merge, and service
address lookup.

### Microservice view

`pkg/mcs/discovery` and `pkg/mcs/registry` provide service discovery and API
registration. Individual service packages own their server lifecycle, primary
election, metadata watches, and gRPC APIs.

## Process Lifecycle And Startup Sequencing

Important anchors:

1. `keyspace.NewKeyspaceManager`
2. `Manager.Bootstrap`
3. `Manager.CreateKeyspace`
4. `Manager.UpdateKeyspaceConfig`
5. `Manager.UpdateKeyspaceState`
6. `keyspace.NewKeyspaceGroupManager`
7. `GroupManager.Bootstrap`
8. `GroupManager.SplitKeyspaceGroupByID`
9. `GroupManager.MergeKeyspaceGroups`
10. `pkg/mcs/discovery/register.go`
11. `pkg/mcs/*/server/server.go`

In classic PD, keyspace managers are created during `server.startServer`. In
keyspace group mode, PD also initializes service primary watchers. Independent
microservices register themselves and elect primaries through etcd.

Maintenance rules:

- Keyspace group bootstrap happens after other `RaftCluster` startup steps to
  avoid stuck goroutines on partial failure.
- Keyspace metadata mutation must hold the appropriate keyed lock.
- Microservice fallback behavior must be explicit and observable.

## Data Model And Metadata Contracts

Hot contracts:

- keyspace ID, name, state, created time, state changed time
- keyspace config keys such as user kind, TSO keyspace group ID, GC management
  type, region bound type, and meta-service group fields
- keyspace group membership and split/merge state
- meta-service group assignment
- service registry entries and primary keys
- expected primary flags
- service-independent markers in cluster state

Maintenance rule:

- Keyspace metadata is user-visible control-plane state. Compatibility and
  rollback behavior matter even when the change is internal.

## Observability And Operational Signals

Open these first:

- `pkg/keyspace/metrics.go`
- `pkg/mcs/*/server/metrics.go`
- `pkg/mcs/discovery`
- `server/apiv2/handlers/keyspace.go`
- `server/apiv2/handlers/microservice.go`

Signals to preserve:

- keyspace operation logs and errors
- service discovery and primary election logs
- fallback state transitions
- resource manager service limit and token metrics
- scheduling and TSO service health signals

## Change Management Guidance

- Keyspace config changes need storage, API, and TSO routing review.
- Keyspace group split/merge changes need concurrency and retry review.
- Microservice primary changes need failure-path tests and discovery review.
- Resource manager changes should be reviewed with service middleware and
  client compatibility.
- Scheduling service changes should be reviewed with classic scheduling
  fallback.

## Must-Read File Order

1. `pkg/keyspace/keyspace.go`
2. `pkg/keyspace/tso_keyspace_group.go`
3. `pkg/keyspace/meta_service_group.go`
4. `pkg/storage/endpoint/keyspace.go`
5. `pkg/storage/endpoint/tso_keyspace_group.go`
6. `pkg/mcs/discovery/register.go`
7. `pkg/mcs/discovery/discover.go`
8. `pkg/mcs/registry/registry.go`
9. `pkg/mcs/server/server.go`
10. `pkg/mcs/tso/server/server.go`
11. `pkg/mcs/scheduling/server/server.go`
12. `pkg/mcs/resourcemanager/server/server.go`
13. `pkg/mcs/router/server/server.go`

## Review Checklist

- Is the keyspace mutation serialized on the correct key?
- Are name and ID lookup caches updated consistently with storage?
- Does the change preserve reserved/default keyspace behavior?
- Does keyspace group movement preserve TSO routing correctness?
- Are split and merge operations retry-safe?
- Does microservice discovery distinguish no service, stale service, and
  current primary states?
- Does fallback behavior match config and leave clear metrics or logs?
