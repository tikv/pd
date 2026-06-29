# Config

This guide covers PD configuration in `server/config/` and the runtime
configuration contracts consumed by server, cluster, scheduling, keyspace, TSO,
and resource manager code.

## Purpose And Scope

Config owns:

- static config parsing, defaults, validation, and embedded etcd config
  generation
- persisted options that can be loaded from and saved to storage
- runtime-safe accessors for scheduling, replication, PD server, keyspace,
  microservice, store, and replication-mode config
- service middleware config and persisted service middleware options
- config API response compatibility
- controller config wiring for resource manager

Config does not own the side effects of every option. The subsystem that
consumes a config value owns the corresponding runtime behavior.

## Core Concepts

- Static config is loaded from flags and TOML before the server starts.
- Persisted options are stored through PD storage and reloaded by the leader.
- Runtime side effects belong to the consuming subsystem, not to config parsing.
- Dynamic config changes must keep storage, in-memory accessors, API responses,
  and metrics aligned.
- Some options are temporary or TTL-scoped; review the override lifetime before
  changing reload behavior.

## Architectural Views

### Static config view

`Config` is the server-level structure loaded from flags and TOML. It includes
listen URLs, initial cluster settings, leader lease, logging, metrics,
scheduling, replication, PD server options, security, dashboard, replication
mode, keyspace, microservice, resource manager controller, and metering config.

### Persisted option view

`PersistOptions` wraps dynamically persisted settings and exposes thread-safe
accessors. It uses `atomic.Value` for most config groups, an atomic pointer for
cluster version, and TTL state for temporary config overrides.

### Runtime side-effect view

Updating persisted config is not enough by itself. Consumers must still apply
runtime effects: scheduler limits, placement rule behavior, store config sync,
microservice fallback, TSO settings, keyspace behavior, or resource manager
limits.

## Process Lifecycle And Startup Sequencing

Important anchors:

1. `server/config/config.go`
2. `server/config/persist_options.go`
3. `server/config/service_middleware_config.go`
4. `server/config/service_middleware_persist_options.go`
5. `server/config/util.go`
6. `server/server.go`
7. `server/api/config.go`

Startup path:

1. Parse and validate static config.
2. Generate embedded etcd config.
3. Create `PersistOptions` from static config.
4. Start the server and reload persisted config after leader campaign.
5. Subsystems consume config through shared providers and live accessors.

Maintenance rules:

- Do not add a config field without a default, validation story, API exposure
  decision, and runtime side-effect owner.
- Do not mutate config structs in place when existing code expects clone/store
  semantics.
- Persisted config reload must not silently diverge from in-memory state.

## Data Model And Metadata Contracts

Hot contracts:

- TOML tags and JSON tags in `Config`
- exported config structs returned by HTTP APIs
- `PersistOptions` accessor semantics
- scheduler config keys and TTL override keys
- replication config and placement-rule enablement
- PD server config values such as region storage and flow rounding
- microservice fallback and dynamic switching config
- store config reported by TiKV and synced by PD
- controller config used by resource manager

Maintenance rule:

- A config rename or default change is a compatibility change. Check command
  line flags, TOML files, HTTP APIs, persisted storage, and tests together.

## Observability And Operational Signals

Open these first:

- `server/config/metrics.go`
- `server/api/config.go`
- `server/cluster/cluster.go`
- `pkg/schedule/metrics.go`
- `pkg/mcs/resourcemanager/server/metrics.go`

Signals to preserve:

- config update logs
- schedule config metrics
- cluster version and metadata gauges
- resource controller config behavior
- errors returned by config APIs

## Change Management Guidance

- Static config changes need validation, default, docs, and startup tests.
- Dynamic config changes need persisted storage, API, and runtime side-effect
  tests.
- Scheduling config changes need scheduler/checker/operator review.
- Replication config changes need placement and cluster metadata review.
- Microservice config changes need fallback and service discovery review.
- Resource manager controller config changes need token bucket and metadata
  watcher review.

## Must-Read File Order

1. `server/config/config.go`
2. `server/config/persist_options.go`
3. `server/config/util.go`
4. `server/config/service_middleware_config.go`
5. `server/config/service_middleware_persist_options.go`
6. `server/api/config.go`
7. `server/server.go`
8. `server/cluster/cluster.go`
9. `pkg/schedule/config/config.go`
10. `pkg/schedule/config/store_config.go`
11. `pkg/mcs/resourcemanager/server/config.go`

## Glossary

- Static config:
  startup-only configuration loaded from command line flags and TOML.
- Persisted option:
  dynamic setting saved in storage and exposed through `PersistOptions`.
- Runtime side effect:
  in-memory behavior that must change after a persisted option is updated.
- TTL override:
  temporary config value that expires after a configured lifetime.
- Config provider:
  shared interface used by subsystems to read current config safely.
- Clone/store semantics:
  pattern where config structs are copied before mutation and atomically
  replaced for readers.

## Review Checklist

- Does the new or changed field have a default and validation?
- Is the field intended to be static, dynamic, or persisted with TTL?
- Are JSON/TOML names stable and compatible?
- Does a persisted update apply the matching runtime side effect?
- Are clone and atomic-store semantics preserved?
- Does the change affect microservice fallback or keyspace group behavior?
- Are API responses and client expectations still compatible?
