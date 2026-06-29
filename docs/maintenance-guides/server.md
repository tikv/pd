# Server

This guide covers the classic PD server process in `server/`.

## Purpose And Scope

`server.Server` owns process-level orchestration:

- embedded etcd startup and client creation
- PD member initialization and leader election
- gRPC and HTTP service registration
- storage, TSO, keyspace, cluster, metering, GC, and heartbeat stream wiring
- service loops and leader callbacks
- shutdown ordering and resource cleanup

It does not own scheduler algorithms, region cache internals, or timestamp
oracle rules. Those are covered by the scheduling, cluster, storage, and TSO
guides.

## Architectural Views

### Bootstrap view

`CreateServer` builds an uninitialized server from config, handler builders,
service labels, rate limiters, registry hooks, and embedded etcd configuration.
`Run` starts embedded etcd, initializes the runtime services, starts monitoring,
and launches server loops.

### Leadership view

`leaderLoop` and `campaignLeader` coordinate PD leadership. The PD leader lease
gates leader-only services, TSO initialization, cluster startup, and leader
callbacks.

### Service view

The embedded etcd gRPC server registers PD, keyspace, diagnostics, resource
manager proxy, and any installed microservice gRPC services. HTTP handlers are
installed through the normal PD API routers and microservice registry hooks.

## Process Lifecycle And Startup Sequencing

Important startup anchors:

1. `CreateServer`
2. `Run`
3. `startEtcd`
4. `startClient`
5. `initMember`
6. `startServer`
7. `startServerLoop`
8. `leaderLoop`
9. `campaignLeader`

`startServer` initializes:

- cluster ID and member metadata
- ID allocator
- encryption manager
- etcd-backed default storage
- LevelDB-backed region storage
- core storage wrapper
- embedded TSO allocator
- `BasicCluster` and `RaftCluster`
- keyspace and keyspace group managers
- metering writer
- GC state manager
- heartbeat streams
- hot-region storage
- HTTP and gRPC rate limiters

Shutdown is centered on `Close`, which stops server loops first, then closes
owned managers, clients, embedded etcd member state, heartbeat streams, storage,
rate limiters, callbacks, and client connections.

Maintenance rules:

- Do not add leader-only behavior before the leader lease is kept.
- Do not start background loops without a matching cancellation path and
  `WaitGroup` ownership.
- Do not assume `startServer` means this member is serving as PD leader.

## Data Model And Metadata Contracts

Server-owned contracts include:

- member name, ID, advertise client URL, advertise peer URL
- member deploy path, binary version, and git hash
- cluster ID initialization
- PD leader key and lease
- service labels and rate limiter state
- service primary watchers in keyspace group mode
- server start timestamp and running state

Changing these contracts can affect API responses, service discovery, PD client
behavior, and upgrade compatibility.

## Observability And Operational Signals

Open these first:

- `server/metrics.go`
- `server/server.go`
- `server/api/health.go`
- `server/api/status.go`
- `server/apiv2/handlers/ready.go`
- `pkg/basicserver`

Signals to preserve:

- PD server info and start timestamp gauges
- embedded etcd state gauges
- leader election logs and lease timing logs
- service forwarding and rate limiter metrics
- health and readiness API behavior

## Change Management Guidance

- Startup and shutdown changes should include a failure-path review.
- Leader election changes should be reviewed with TSO and cluster startup
  behavior.
- Service registration changes should be reviewed with API and client
  compatibility.
- Config changes should be reviewed with persisted config loading and runtime
  side effects.

## Must-Read File Order

1. `server/server.go`
2. `server/handler.go`
3. `server/grpc_service.go`
4. `server/api/router.go`
5. `server/apiv2/router.go`
6. `server/config/config.go`
7. `server/config/persist_options.go`
8. `pkg/member/member.go`
9. `pkg/member/election.go`
10. `pkg/utils/etcdutil`

## Review Checklist

- Does the change run only on the intended ownership state:
  follower, PD leader, etcd leader, service primary, or all members?
- Are contexts, timers, watchers, and goroutines stopped on every exit path?
- Can `Close` run after a partial startup failure?
- Does the change preserve leader lease timing and TSO readiness?
- Are HTTP/gRPC handlers registered before metrics or label initialization uses
  them?
- Does the change need a config reload or persisted option migration?
- Are failpoint-aware tests used for startup, leader transfer, and shutdown
  paths?
