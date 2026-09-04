# PD region meta consistency check

`pd-ctl region meta-consistency` compares the region meta cached locally by the PD leader and every follower in the same cluster. It scans each PD instance directly in small batches, starts the same batch concurrently across instances, keeps requests serial per instance, applies a global rate limit, and produces one JSON report.

The command uses the `/members`, `/regions/count`, `/regions/key`, and `/region/id/{id}` APIs and depends on follower-local read headers. Compatibility tests cover current master and [`v8.5.4-20260625-8b1b130`](https://github.com/tikv/pd/releases/tag/v8.5.4-20260625-8b1b130). Verify these API semantics before using the command with another PD version.

> A full scan adds PD HTTP processing, JSON serialization, network traffic, and short-lived Region tree read-lock work. The defaults bound these costs. Do not run this command when any production impact is unacceptable.

## Quick start

Run the command from a separate operations host in the same VPC and use a new output file for every check:

```bash
report="region-meta-report-$(date -u +%Y%m%dT%H%M%SZ).json"
set +e
pd-ctl -u http://10.0.0.1:2379 region meta-consistency \
  --output "$report"
rc=$?
set -e

printf 'exit_code=%s report=%s\n' "$rc" "$report"
test ! -s "$report" || jq '{status, summary, confirmation, nodes}' "$report"
```

The supplied URL is only a seed. The command discovers the cluster through `/pd/api/v1/members` and connects directly to each advertised `client_urls` address. Do not use a load balancer address in place of direct member addresses.

| Exit code | Status | Meaning |
| ---: | --- | --- |
| `0` | `consistent` | No difference remained in this observation. |
| `1` | `inconsistent` | The check completed and confirmed at least one stable difference. |
| `2` | `incomplete` or no new report | The evidence was insufficient or execution failed. Inspect stderr and do not reuse an older report. |
| `130` | No new report | The user interrupted the command with Ctrl-C. |

Exit code `1` means that the command found an inconsistency; it does not mean that command execution failed.

## Prerequisites

- The cluster has at least two PD members, and the execution host can connect directly to every advertised `client_urls` address.
- The follower Region Syncer is healthy and follower-local reads with `PD-Allow-Follower-Handle: true` are available.
- PD CPU, memory, request latency, and existing alerts have clear headroom. Run the check during an off-peak window.
- No PD restart, scale operation, or planned PD leader transfer occurs during the scan.
- The `--work-dir` and output directories already exist, are writable, and have enough free space. Treat the report as internal diagnostic data.

The minimum recommended independent checker host is `1 vCPU / 512 MiB RAM / 3 GiB free disk / 100 Mbps`. Prefer `2 vCPU / 1 GiB RAM / 5 GiB free disk`.

## Compared fields

| Category | Fields |
| --- | --- |
| Region | Region ID |
| Key range | `start_key`, `end_key` |
| Epoch | `conf_ver`, `version` |
| Peers | `id`, `store_id`, `role`, `is_witness` |
| Region leader peer | `id`, `store_id`, `role`, `is_witness` |

Heartbeat statistics such as traffic, size, `pending_peers`, `down_peers`, and buckets are not compared. Peer array order is also ignored. The report's `reference` identifies the PD leader at the start of the scan; it does not assert that this member's data is correct. `leader_peer` means the Region leader peer, not the PD leader.

## Workflow and result boundaries

1. Read the member list, PD leader, and cluster ID, then establish a direct HTTP connection to every PD instance.
2. Read Region counts from all instances and page through `/pd/api/v1/regions/key` by key range. Requests for the same batch start as close together as possible.
3. Keep at most one request in flight per instance. All instances share one request budget: a group containing N requests consumes N budget units, so the default long-term aggregate rate does not exceed 20 requests/s.
4. Send `PD-Allow-Follower-Handle: true`, `PD-Redirector: pd-ctl-region-meta-consistency`, and `X-Caller-ID: pd-ctl` so each target instance serves its local Region cache.
5. Release matching data immediately. Write only differences to bounded temporary JSONL files and externally merge-sort them by Region ID.
6. Wait one second and concurrently recheck up to `--confirm-limit` differences with the smallest Region IDs. Verify the PD leader, cluster ID, and membership again before producing the result.

A member scan is accepted only when its Region count before the scan, number of scanned Regions, and Region count after the scan are equal. The default does not retry the full scan, which avoids multiplying load while Region metadata is changing continuously.

Independent HTTP requests cannot form a distributed atomic snapshot:

- `consistent` means that no difference remained in this observation. It is not proof of linearizable equality.
- `inconsistent` means that at least one rechecked difference remained unchanged, which is strong evidence of divergence.
- `incomplete` means that the evidence is insufficient and must not be interpreted as consistency.

## Parameters

Run `pd-ctl region meta-consistency --help` for the authoritative parameter list.

| Parameter | Default with unit | Description |
| --- | ---: | --- |
| `-u, --pd` | `http://127.0.0.1:2379` (URL) | One seed URL. User information, path, query, and fragment are rejected. |
| `--batch-size` | `128 Regions/request` | Maximum Regions per request; range `1..1024 Regions/request`. |
| `--interval` | `50ms/request` | Interval for each global HTTP request budget unit. Use a Go duration such as `100ms` or `1s`. A three-member batch has a minimum group interval of `150ms`. |
| `--timeout` | `10s/request` | Timeout for one HTTP request. |
| `--max-runtime` | `4h/check` | Wall-clock hard limit for the complete check. |
| `--retries` | `0 retries/request` | Additional retries for one request; range `0..10`. |
| `--scan-retries` | `0 retries/check` | Full-cluster retries after a Region count change; range `0..3`. |
| `--confirm-limit` | `128 Regions/check` | Maximum differences to recheck; range `0..1024`. Set to `0` to disable confirmation. |
| `--work-dir` | System temporary directory (path) | Directory for temporary differences and a temporary stdout report. |
| `--max-temporary-disk-mib` | `1024 MiB` | Hard limit for temporary difference sorting and merging. |
| `--max-output-mib` | `1024 MiB` | Hard limit for the final JSON report. |
| `--output` | `-` (stdout or path) | `-` writes to stdout. A file path is atomically replaced after the complete report is written. |
| `--cacert` | System CA (path) | HTTPS CA bundle. |
| `--cert`, `--key` | Unset (path) | mTLS client certificate and private key. Both must be supplied together. |
| `--authorization-file` | Unset (path) | File containing one complete Authorization header value. Authorization is sent only over HTTPS. |

`--interval 0` is only for isolated test environments with no production traffic and must not be used in production. If controlled diagnosis is permitted but cluster load is elevated, reduce both per-request and sustained load:

```bash
pd-ctl -u http://10.0.0.1:2379 region meta-consistency \
  --batch-size 64 \
  --interval 100ms \
  --output region-meta-report.json
```

## Output

The report is compact single-line JSON.

| Field | Meaning |
| --- | --- |
| `status` | `consistent`, `inconsistent`, or `incomplete`. |
| `reference` | Name, ID, and URL of the PD leader at scan start. |
| `settings` | Rate limit, concurrency, hard limits, HTTP request count, response body bytes, and peak temporary disk usage. |
| `nodes` | Name, address, role, Region count, batch count, scan attempts, and timestamps for every PD member. |
| `confirmation` | Recheck scope, stable/resolved/changed Region IDs, and the number of unconfirmed Regions. |
| `summary` | Total differing Regions and counts by differing field. |
| `differences` | Every retained difference, sorted by Region ID. |

Every difference contains `region_id` and only fields that actually differ: `missing_on`, `key_range`, `epoch`, `peers`, and `leader_peer`. Instances use `<PD member name>@<host:port>`. Peer role values are `0=Voter`, `1=Learner`, `2=IncomingVoter`, and `3=DemotingVoter`.

Key fields in a consistent report:

```json
{
  "status": "consistent",
  "summary": {"different_regions": 0, "by_field": {}},
  "confirmation": {"result": "not_needed", "final_differences": 0},
  "differences": []
}
```

Key fields in an inconsistent report:

```json
{
  "status": "inconsistent",
  "summary": {"different_regions": 2, "by_field": {"missing_on": 1, "epoch": 1}},
  "confirmation": {
    "result": "stable",
    "checked_regions": 2,
    "stable_regions": [42, 43],
    "final_differences": 2
  },
  "differences": [
    {
      "region_id": 42,
      "epoch": {
        "pd-0@10.0.0.1:2379": {"conf_ver": 3, "version": 8},
        "pd-1@10.0.0.2:2379": {"conf_ver": 3, "version": 7}
      }
    },
    {"region_id": 43, "missing_on": ["pd-1@10.0.0.2:2379"]}
  ]
}
```

Common queries:

```bash
jq '{status, summary, confirmation}' region-meta-report.json
jq -r '.differences[].region_id' region-meta-report.json
jq '.differences[] | select(has("missing_on") or has("key_range") or has("epoch"))' region-meta-report.json
jq '.differences[] | select(has("peers") or has("leader_peer"))' region-meta-report.json
```

## Resources and capacity

The command retains at most one current response per PD member, with an 8 MiB hard limit for each response. Difference sorting uses a fixed 8 MiB buffer, confirmation retains at most `--confirm-limit` candidates, and the report is written incrementally. Memory usage therefore depends mainly on the number of PD members, batch size, current responses, and confirmation limit; it does not grow linearly with the total Region count. More Regions primarily increase request count, cumulative network traffic, and runtime.

| Resource | Minimum | Recommended | Notes |
| --- | ---: | ---: | --- |
| CPU | 1 vCPU | 2 vCPU | Network reads can match the PD member count; comparison and report generation run in the main process. |
| Memory | 512 MiB | 1 GiB | Covers three concurrent current pages, JSON decoding, and the fixed sorting buffer. |
| Free disk | 3 GiB | 5 GiB | Covers the default 1 GiB temporary limit, 1 GiB report limit, and filesystem headroom. |
| Network | 100 Mbps | Same VPC or Availability Zone as PD | Must reach every PD address directly. |
| Task window | 4 h | More than 4 h | The default runtime hard limit is 4 h. |

If `--work-dir` and the output directory are on different filesystems, reserve the corresponding capacity on each filesystem.

### Million-Region benchmark

The capacity data below uses the PD API from [`v8.5.4-20260625-8b1b130`](https://github.com/tikv/pd/releases/tag/v8.5.4-20260625-8b1b130). The test host was an AWS EC2 `r7i.4xlarge` with 16 vCPUs, 123 GiB visible memory, no swap, and Ubuntu 22.04.5. The topology contained three PD members and three simulated stores, with three peers per Region. Every scale used a fresh data directory. Heartbeats were stopped before each check, all three members had equal Region counts, and Region Syncer indexes had caught up. The `pd-ctl` binary was built with Go 1.26.5 from commit `ecbbc0cea2ee8507512a7384e1e015e349e911ab`; its SHA-256 was `248cba827138361ecaa6cf8456a0e90492dcd0bfe2edf2a1b4624643a0b95d05`. Re-measure other PD versions and data models in an equivalent environment.

| Regions per PD | HTTP requests | Response body | Default rate-limit lower bound | `interval=0` wall time | Checker peak RSS |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 23,447 | 1.24 GiB | 19 min 32 s | 15.187 s | 42.25 MiB |
| 2,000,000 | 46,883 | 2.48 GiB | 39 min 04 s | 29.729 s | 42.25 MiB |
| 4,000,000 | 93,758 | 4.99 GiB | 1 h 18 min 08 s | 59.849 s | 43.84 MiB |
| 8,000,000 | 187,508 | 10.02 GiB | 2 h 36 min 15 s | 123.029 s | 43.02 MiB |

| Regions per PD | Combined PD CPU time | Maximum per-PD RSS (before to after) | Report size |
| ---: | ---: | ---: | ---: |
| 1,000,000 | 21.08 s | 2.21 GiB to 2.67 GiB | 1,567 B |
| 2,000,000 | 32.91 s | 4.50 GiB to 5.41 GiB | 1,569 B |
| 4,000,000 | 63.80 s | 9.78 GiB to 11.20 GiB | 1,571 B |
| 8,000,000 | 196.51 s | 16.15 GiB to 18.30 GiB | 1,572 B |

All four scales returned `consistent`. Every member completed one scan without request retries or full-scan retries, `differences` was empty, and temporary difference disk usage was `0`. `interval=0` measures the isolated upper-throughput case and must not be used as a production setting. Production runtime is governed by the default global request budget. With 8,000,000 Regions, aggregate average response body traffic across the three PD members was approximately 1.10 MiB/s under the default budget.

Checker RSS remains stable because `http_response_bytes` is cumulative for the complete run, while memory holds only each member's current page, the fixed sorting buffer, and bounded confirmation candidates. PD still scans the Region tree and serializes JSON for every request. In the unthrottled test, peak RSS for one PD increased by as much as 3.76 GiB. Prefer a separate operations host. If the checker must share a PD host, reserve at least one idle logical CPU, 6 GiB `MemAvailable`, and 3 GiB on a non-PD data disk in addition to checker resources. The 8,000,000-Region test model requires at least 32 GiB on a PD host. Real key lengths, peer counts, workload, and PD configuration can change these requirements; validate headroom in an equivalent environment before execution.

## Production safeguards

- Use the production defaults. Do not increase the batch size, remove rate limiting, or enable full-scan retries first merely to shorten runtime.
- Monitor PD CPU, Go heap and GC, request latency, existing alerts, `pd_region_syncer_status{type="sync_index"}`, and `pd_region_syncer_status{type="last_index"}` against existing production thresholds.
- Press Ctrl-C immediately if any existing SLO, alert, or resource threshold is reached. Do not relax thresholds during the check.
- The report contains PD addresses, Region IDs, key ranges, Peer IDs, and Store IDs. Do not publish it.
- Restrict the Authorization file to the current user, transmit it only over HTTPS, and handle it according to organizational security requirements.

Progress is written to stderr every 100 scan batches and never contaminates JSON stdout.

## Tests

Core tests cover consistent metadata, missing Regions, key range, epoch, peers, Region leader peers, role, witness state, `uint64` values, transient differences, Region count growth and retry, membership changes, concurrent same-batch requests, global request budgeting, response/disk/output limits, and external sorting:

```bash
make -C tools gotest \
  GOTEST_ARGS='./pd-ctl/pdctl/command/regionmeta -count=1'
```

The three-PD integration test injects Region heartbeats, mutates one follower's local cache, and verifies exact epoch and missing-Region differences:

```bash
make -C tools gotest \
  GOTEST_ARGS='-tags=without_dashboard ./pd-ctl/tests/region -run TestRegionMetaConsistencyUsesFollowerLocalCache -count=1'
```
