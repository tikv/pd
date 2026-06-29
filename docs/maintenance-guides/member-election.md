# Member And Election

This guide covers PD membership, leader election, service primary election, and
etcd utilities in `pkg/member/`, `pkg/election/`, and `pkg/utils/etcdutil/`.

## Purpose And Scope

Member and election code owns:

- PD member identity and member metadata
- PD leader key and lease ownership
- service participant identity and primary keys for microservices
- leader/primary campaign, watch, resign, and transfer behavior
- etcd client helpers, leader movement, health checks, and watch loops
- serving-state checks used by APIs, TSO, scheduling, and resource manager

It does not own the business logic that becomes active after leadership is
obtained. That belongs to server, TSO, cluster, scheduling, or resource manager.

## Architectural Views

### PD member view

`member.Member` wraps embedded etcd identity, election client, leadership, and
the current PD leader value. `server.leaderLoop` calls `CheckLeader`, aligns
PD leader campaigning with the embedded etcd leader, and calls `Campaign`.

### Service participant view

`member.Participant` generalizes the same election model for microservice
primaries. TSO, scheduling, resource manager, router, and meta-storage services
use participant-style primary election.

### etcd utility view

`pkg/utils/etcdutil` centralizes etcd client creation, cluster ID checks,
member operations, leader transfer, health checks, proto reads, and loop
watchers.

## Process Lifecycle And Startup Sequencing

Important anchors:

1. `server.startClient`
2. `server.initMember`
3. `server.leaderLoop`
4. `server.campaignLeader`
5. `member.NewMember`
6. `Member.CheckLeader`
7. `Member.Campaign`
8. `Member.Resign`
9. `member.Participant`
10. `etcdutil.NewLoopWatcher`

Classic PD sequence:

1. Start embedded etcd.
2. Create separate etcd clients for server metadata and election operations.
3. Initialize PD member info with the embedded etcd server ID.
4. Check the persistent PD leader key.
5. Watch an existing leader or campaign when no valid leader exists.
6. Keep the leadership lease before enabling leader-owned services.
7. Resign and reset serving state when leadership ends.

Maintenance rules:

- Do not conflate embedded etcd leader, PD leader, and microservice primary.
- Serving checks must use lease-backed leadership, not just cached leader
  identity.
- Watch loops must have context cancellation and load/error behavior that
  callers can reason about.

## Data Model And Metadata Contracts

Hot contracts:

- PD member ID, name, member value, client URLs, peer URLs, binary version, git
  hash, and deploy path
- election path for PD leader
- primary paths for microservices
- expected primary flags used by transfer APIs
- leader key revision and delete-by-revision behavior
- etcd cluster ID and member list
- leadership lease TTL and campaign frequency guard

Maintenance rule:

- Election keys are coordination contracts. Changing their path, value, lease,
  or delete behavior can split ownership across processes.

## Observability And Operational Signals

Open these first:

- `pkg/member/metrics.go`
- `pkg/utils/etcdutil/metrics.go`
- `server/server.go`
- `pkg/mcs/*/server/server.go`

Signals to preserve:

- member and service role gauges
- leader/primary campaign logs
- leader watch and leader-change logs
- etcd leader transfer logs
- loop watcher load and watch errors
- etcd health signals

## Change Management Guidance

- PD leader election changes must be reviewed with TSO, cluster startup, and
  server readiness.
- Microservice primary changes must be reviewed with service discovery and
  fallback behavior.
- etcd client changes must preserve separate election and metadata client
  purposes.
- Loop watcher changes need tests for initial load, live put/delete, compaction,
  cancellation, and error retry behavior.

## Must-Read File Order

1. `pkg/member/member.go`
2. `pkg/member/participant.go`
3. `pkg/member/election.go`
4. `pkg/election`
5. `pkg/utils/etcdutil/etcdutil.go`
6. `pkg/utils/etcdutil/health_checker.go`
7. `server/server.go`
8. `pkg/mcs/tso/server/server.go`
9. `pkg/mcs/scheduling/server/server.go`
10. `pkg/mcs/resourcemanager/server/server.go`
11. `pkg/mcs/utils/expected_primary.go`
12. `pkg/utils/keypath`

## Review Checklist

- Which ownership state is required: etcd leader, PD leader, or service
  primary?
- Is the leadership lease kept before serving starts?
- Does the code resign and clear serving state on every failure path?
- Are campaign retries bounded or backed off where needed?
- Does leader transfer respect expected-primary semantics?
- Can loop watchers recover from transient etcd errors?
- Are member values and key paths compatible across rolling upgrade?
