# Resource Manager

This guide covers resource manager code under `pkg/mcs/resourcemanager/` and
its integration with the classic PD process.

## Purpose And Scope

Resource manager owns:

- resource group metadata and state
- RU token bucket calculation
- service limits per keyspace
- controller config and RU version policy
- resource group metadata watching
- resource manager gRPC and REST service registration
- independent resource manager primary election
- embedded-PD fallback and proxy behavior
- RU consumption metrics and metering collection

It does not own TiDB-side admission control, but TiDB relies on these contracts
for resource group behavior.

## Core Concepts

- Resource manager owns PD-side resource-group metadata, RU token buckets,
  service limits, and metering collection.
- Token bucket state is runtime behavior; resource group metadata and states are
  persisted contracts.
- Metadata-write and state-write roles are separate and depend on embedded PD
  versus independent resource-manager mode.
- Metadata watcher initialization must produce the same cache meaning as full
  storage load.
- The default resource group and reserved keyspace behavior must stay
  idempotent across startup, watch replay, and API writes.

## Architectural Views

### Manager view

`Manager` owns controller config, keyspace resource group managers, resource
group metadata, service limiters, consumption dispatch, metrics, and RU
collector registration. It initializes through server lifecycle callbacks in
embedded mode or service startup in independent mode.

### Resource group view

`ResourceGroup` wraps resource group settings, priority, runaway/background
settings, RU consumption, and a `GroupTokenBucket`. Settings can be patched or
applied depending on whether token deltas should be preserved.

### Token bucket view

`GroupTokenBucket` and token slots split RU across clients in the same resource
group, apply burst modes, preserve reserved burst/service tokens, and produce
minimum trickle times for clients.

### Metadata watcher view

Independent resource manager can initialize through a metadata watcher instead
of full storage load. The watcher applies controller config, resource group
settings, resource group states, and service limits from etcd events.

## Process Lifecycle And Startup Sequencing

Important anchors:

1. `pkg/mcs/resourcemanager/server.NewManager`
2. `Manager.Init`
3. `Manager.initMetadata`
4. `Manager.initializeMetadataWatcher`
5. `Manager.backgroundMetricsFlush`
6. `Manager.persistLoop`
7. `Service.AcquireTokenBuckets`
8. `pkg/mcs/resourcemanager/server.Server.Run`
9. `Server.primaryElectionLoop`
10. `server/resource_group_proxy_service.go`

Embedded PD sequence:

1. Server start callback creates storage and registers the RU collector.
2. Service-ready callback initializes metadata after PD becomes serving.
3. Background metrics flushing starts.
4. State persistence starts when the write role allows state writes.

Independent service sequence:

1. Resource manager service starts and registers with service discovery.
2. It campaigns for resource manager primary.
3. Primary callbacks initialize manager state.
4. Metadata watcher or full load populates local caches.

Maintenance rules:

- Metadata writes and state writes are controlled separately by write role.
- gRPC metadata writes may be rejected in modes where PD owns metadata.
- The default resource group and reserved keyspace behavior must remain
  idempotent.

## Data Model And Metadata Contracts

Hot contracts:

- `resource_manager.ResourceGroup`
- group mode, priority, RU settings, token limit settings, runaway settings,
  background settings, and RU consumption
- keyspace ID extraction and null/default keyspace behavior
- controller config and RU version policy
- resource group settings and states in storage
- service limits per keyspace
- token bucket fill rate, burst limit, override fill rate, override burst
  limit, token slots, reserved tokens, and last update time
- metadata watcher key parsing under resource group prefixes

Maintenance rule:

- Resource group APIs are control-plane contracts with TiDB. Changes must keep
  metadata persistence, runtime token behavior, and client compatibility aligned.

## Observability And Operational Signals

Open these first:

- `pkg/mcs/resourcemanager/server/metrics.go`
- `pkg/mcs/resourcemanager/server/metering.go`
- `pkg/mcs/resourcemanager/server/grpc_service.go`
- `pkg/mcs/resourcemanager/server/manager.go`
- `pkg/mcs/resourcemanager/redirector`

Signals to preserve:

- token bucket request metrics
- consumption and RU metrics
- background metrics flushing
- resource manager primary election logs
- metadata watcher logs
- service limit and controller config update logs
- metering collector output

## Change Management Guidance

- Token bucket changes need tests for burst modes, slot balancing, refill,
  reserved tokens, and trickle time.
- Metadata changes need watcher, full-load, write-role, and persistence tests.
- gRPC changes need TiDB compatibility review.
- Service limit changes need keyspace behavior and override-token review.
- Primary election changes need failure-path tests that avoid half-initialized
  primaries.

## Must-Read File Order

1. `pkg/mcs/resourcemanager/server/manager.go`
2. `pkg/mcs/resourcemanager/server/resource_group.go`
3. `pkg/mcs/resourcemanager/server/token_buckets.go`
4. `pkg/mcs/resourcemanager/server/service_limit.go`
5. `pkg/mcs/resourcemanager/server/metadata_watcher.go`
6. `pkg/mcs/resourcemanager/server/grpc_service.go`
7. `pkg/mcs/resourcemanager/server/server.go`
8. `pkg/mcs/resourcemanager/server/config.go`
9. `pkg/mcs/resourcemanager/redirector/redirector.go`
10. `pkg/mcs/resourcemanager/metadataapi/config_service.go`
11. `server/resource_group_metadata_manager.go`
12. `server/resource_group_proxy_service.go`

## Glossary

- Resource group:
  TiDB-facing control-plane object defining RU settings and behavior.
- RU:
  request unit consumed by SQL workload and refilled by token buckets.
- Token bucket:
  runtime structure that meters RU allocation to clients.
- Service limit:
  per-keyspace limit state used to bound resource-manager service behavior.
- Write role:
  policy deciding whether this manager can write metadata, state, or both.
- Metadata watcher:
  etcd watcher that keeps resource group caches in sync from events.
- Metering:
  collection and writing of resource usage records.

## Review Checklist

- Does the change respect metadata-write and state-write roles?
- Are embedded PD and independent resource manager paths both correct?
- Does metadata watcher behavior match full-load behavior?
- Is the default resource group initialized idempotently?
- Are token bucket calculations stable across setting changes and time jumps?
- Are keyspace-specific and legacy resource group paths both handled?
- Are metrics and metering updated with behavior changes?
