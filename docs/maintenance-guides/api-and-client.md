# API And Client

This guide covers PD's external API boundary and client compatibility surface.

## Purpose And Scope

Covered paths:

- gRPC services in `server/grpc_service.go`
- REST APIs in `server/api/`
- v2 APIs and middleware in `server/apiv2/`
- PD client module in `client/`
- request forwarding to leaders or split microservices
- API response compatibility and error semantics

This guide does not define scheduler or storage behavior, but API changes often
need review with those subsystem guides.

## Architectural Views

### gRPC view

`GrpcServer` wraps `server.Server` and implements PD gRPC methods such as TSO,
bootstrap, store heartbeat, region heartbeat, split, scatter, and operator
lookup. Several methods validate leadership, redirect or forward requests, and
then call `RaftCluster`, `TSO`, or scheduler-owned logic.

### HTTP view

`server/api/` exposes the v1 operational API for members, config, stores,
regions, schedulers, rules, operators, health, diagnostics, maintenance, and
debug behavior. `server/apiv2/` adds v2 handlers and middleware for readiness,
keyspace, safe point, microservice redirects, affinity, and maintenance.

### Client view

`client/` is a Go submodule. It owns service discovery, HTTP helpers, TSO and
resource-manager clients, keyspace and meta-storage clients, options, metrics,
and compatibility types shared with external users.

## Process Lifecycle And Startup Sequencing

- gRPC services are registered through the embedded etcd config's
  `ServiceRegister` hook during server creation.
- HTTP handlers are installed through legacy service builders and the
  microservice registry.
- gRPC service labels are initialized after embedded etcd starts and the gRPC
  server service info is available.
- Client-side service discovery watches PD or microservice endpoints and
  updates request routing after leader or primary changes.

Maintenance rule:

- Handler registration is part of process startup. Do not move it without
  checking rate limit labels, audit labels, and readiness behavior.

## Data Model And Metadata Contracts

API-visible contracts include:

- `pdpb.ResponseHeader` and region error semantics
- stream close and timeout behavior for TSO and region heartbeat
- JSON response fields in v1 and v2 handlers
- `client/http/types.go` structures mirrored from server responses
- status types exported from `server/cluster`
- service discovery endpoint format
- HTTP status codes and errcode response shape

Maintenance rule:

- Changing exported response fields or error behavior is a compatibility change
  even when no Go API signature changes.

## Observability And Operational Signals

Open these first:

- `server/metrics.go`
- `server/api/metric.go`
- `server/api/health.go`
- `server/api/status.go`
- `client/metrics/metrics.go`

Signals to preserve:

- forwarding failure counters
- TSO proxy stream counters and timeout logs
- heartbeat stream send/receive errors
- API rate limiter labels
- health and readiness behavior

## Change Management Guidance

- For gRPC changes, review TiKV and TiDB callers through kvproto expectations.
- For HTTP changes, review `client/http` type compatibility and swagger
  annotations if the route is documented.
- For forwarding changes, check leader, primary, retry, and timeout behavior.
- For client changes, run client submodule tests through its make targets.

## Must-Read File Order

1. `server/grpc_service.go`
2. `server/forward.go`
3. `server/api/router.go`
4. `server/api/server.go`
5. `server/api/middleware.go`
6. `server/apiv2/router.go`
7. `server/apiv2/middlewares/redirector.go`
8. `server/apiv2/middlewares/microservice_redirector.go`
9. `client/client.go`
10. `client/servicediscovery/service_discovery.go`
11. `client/http/client.go`
12. `client/http/types.go`

## Review Checklist

- Does the handler require PD leader, any member, or a specific microservice
  primary?
- Is request forwarding bounded by context, timeout, and retry limits?
- Are stream send and receive paths closed on both endpoint errors?
- Are response headers and error fields compatible with existing TiKV and TiDB
  clients?
- Does the HTTP handler use the existing errcode/error response pattern?
- Do new APIs need swagger annotations or client type updates?
- Are audit, rate limit, and service labels updated with route changes?
