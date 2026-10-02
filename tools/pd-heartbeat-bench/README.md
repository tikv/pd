# pd-heartbeat-bench

`pd-heartbeat-bench` benchmarks PD's Region Heartbeat ingestion path with
configurable Region, store, flow, and silent-Region workloads.

1. Deploy a dedicated cluster containing only PD, for example
   `tiup playground nightly --pd 3 --kv 0 --db 0`.
2. Build the tool with `make pd-heartbeat-bench`.
3. Run `bin/pd-heartbeat-bench --config tools/pd-heartbeat-bench/config-template.toml --pd-endpoints <pd-leader-address>`.

Do not point the tool at a production cluster. It bootstraps mock stores and
Regions and may generate scheduler operators.

## Ratio semantics

All ratios use the total Region count as their denominator and describe one
heartbeat round after the initial registration round:

- `report-ratio`: fraction of all Regions that send a heartbeat. The remainder
  are silent Regions, as when Region hibernation is enabled.
- `leader-update-ratio`: fraction of all Regions whose leader changes.
- `epoch-update-ratio`: fraction of all Regions whose epoch version changes.
- `space-update-ratio`: fraction of all Regions whose approximate size or key
  count changes.
- `flow-update-ratio`: fraction of all Regions whose flow statistics change.

Every update set is a subset of the reporting Regions, so each update ratio
must be less than or equal to `report-ratio`. Update sets may overlap: one
Region can change its leader, epoch, space, and flow in the same round.

In other words, `report-count = floor(total-regions * report-ratio)` and each
`update-count = floor(total-regions * update-ratio)`. An update ratio is not
multiplied by `report-ratio` a second time.

The initial round always reports every Region so that PD learns the full
topology. `round` counts subsequent workload rounds; `0` runs indefinitely.

For example, with one million Regions, `report-ratio = 0.1` and
`flow-update-ratio = 0.05`, 100,000 Regions report and 50,000 of them update
flow statistics.

The set of reporting Regions is stable across rounds to model a stable silent
population. It is randomly distributed across the keyspace using
`random-seed`, rather than being one contiguous key range.

## Result semantics

The client-side `region heartbeat client send stats` measure time blocked in
the gRPC stream's `Send` call. They include transport backpressure and are not
PD processing latency. Use the collected
`pd_scheduler_handle_region_heartbeat_duration_seconds` metrics for PD's
server-side processing latency.

`delete-operators` is disabled by default. Enabling it deletes outstanding
operators every 30 seconds and intentionally creates a synthetic scheduler
churn workload. This benchmark does not execute PD operators; use it as an
ingestion benchmark, not as a scheduler convergence simulator.

## HTTP Server
The tool starts an HTTP server based on the StatusAddr field in the configuration file. Ensure that the StatusAddr is correctly configured before starting the server.

## API Endpoints
1. Get Current Configuration

**Endpoint**: `GET /config`

**Description**: Returns the current configuration of the benchmark tool.

Response:

- Status Code: `200 OK`
- Content: JSON representation of the configuration.

Example Response:

```json
{
  "hot-store-count": 10,
  "flow-update-ratio": 0.05,
  "leader-update-ratio": 0.03,
  "epoch-update-ratio": 0.02,
  "space-update-ratio": 0.01,
  "report-ratio": 0.1
}
```

2. Update Configuration

**Endpoint**: `PUT /config`

**Description**: Updates the configuration of the benchmark tool.

Request Body: JSON representation of the new configuration.

Response:

- Status Code: `200 OK` if the update is successful.
- Status Code: `400 Bad Request` if the request body is invalid or fails validation.

Example Request:

```json
{
  "hot-store-count": 15,
  "flow-update-ratio": 0.06,
  "leader-update-ratio": 0.04,
  "epoch-update-ratio": 0.03,
  "space-update-ratio": 0.02,
  "report-ratio": 0.1
}
```
