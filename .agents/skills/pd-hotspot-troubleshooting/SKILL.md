---
name: pd-hotspot-troubleshooting
description: Troubleshooting workflow for PD hotspot investigations in TiDB/TiKV clusters. Use when diagnosing read/write/CPU imbalance, hotspot scheduling inefficiency, QPS or CPU skew not clearly reflected in `hot read/write`, hotspot operator backlog/timeout, and when tuning `balance-hot-region-scheduler` with rollback safety.
---

# PD Hotspot Troubleshooting

## 1) Input Information (Clarify First)

- Incident window: start time, duration, and periodicity.
- Impact scope: read latency, write latency, error types, and business impact.
- Change context: scale operations, DDL, truncate, analyze, CDC/BR.
- Actions already taken: transfer leader, `evict-leader-scheduler`, offline/online operations, table scatter.

## 2) Troubleshooting Order and Priority

1. **PD / TiKV monitoring**
2. **Top SQL and Slow SQL**
3. **PD / TiKV logs**
4. **SQL hotspot and hotspot history views**
5. **`pd-ctl` fallback (internal-access scenarios only)**

Cloud-first principle: prioritize monitoring, Top SQL / Slow SQL, and logs.
Do not start with `pd-ctl` directly. Use externally accessible evidence first.

## 3) Step 1: PD / TiKV Monitoring Investigation (Primary Path)

### 3.1 Capture PD config snapshot from control platform

Before metric/log analysis, capture a PD config snapshot from your control
platform (managed service console or internal control plane):

- scheduler inventory and paused status;
- `balance-hot-region-scheduler` config;
- schedule-level hotspot knobs (for example, `hot-region-schedule-limit` and
  `hot-region-cache-hits-threshold`);
- halt/disable related flags if exposed by the platform.

Use this snapshot as baseline evidence for diagnosis and rollback decisions.

### 3.2 PD monitoring (panel order)

Start with **Region health**:

- Check pending peers / down peers / offline peers / extra peers / missing peers.
- If region health is abnormal, fix health issues before hotspot scheduling analysis.

Then verify scheduler runtime:

- **Scheduler is running**: confirm `balance-hot-region-scheduler` keeps running.
- **Schedulers reach limit**: check whether scheduling is frequently blocked by limits.

Then check operator lifecycle:

- **Schedule operator create / check / finish / timeout / replaced or canceled**.
- Key signal: normal creation but low finish with high timeout usually indicates
  execution bottlenecks, not scheduling logic bottlenecks.

Finally check hotspot scheduling metrics:

- **Statistics - hot write / Statistics - hot read**.
- Focus on:
  - whether hot region leader/peer distribution is overly concentrated;
  - whether store-level read/write bytes/keys/query are imbalanced;
  - whether **Selector write events / Selector read events** are abnormal;
  - whether **Direction of hotspot transfer leader / move peer** shows repeated
    moves without real dispersion.

### 3.3 TiKV monitoring (aligned with PD conclusions)

CPU path:

- First check store-level CPU skew.
- Then inspect thread CPU:
  - `Scheduler worker CPU` (often related to write hotspots);
  - `Unified read pool CPU` (often related to read hotspots);
  - `gRPC poll CPU` (may relate to both read and write hotspots).

Traffic path:

- Check whether MBps trends align with PD hotspot conclusions.
- If PD shows scheduling activity but TiKV shows no improvement, prioritize
  snapshot/disk/network execution bottlenecks.

## 4) Step 2: Top SQL / Slow SQL

If Top SQL is available (Dashboard or SQL entry), this step is mandatory:

- Identify hotspot SQL, hotspot tables, and hotspot indexes.
- Cross-validate with PD/TiKV monitoring evidence.
- Build a causal chain from request pattern to hotspot distribution.

## 5) Step 3: PD / TiKV Logs

Use logs to validate scheduling and execution paths over the incident window:

- PD log focus:
  - hotspot scheduler activity;
  - operator create/finish/timeout/cancel signals;
  - scheduling-limit related signals.
- TiKV log focus:
  - snapshot/raftstore execution pressure;
  - timeout/backoff related signals around hotspot periods;
  - disk/network stress signals that block operator completion.

## 6) Step 4: SQL Hotspot and History Views

Prefer SQL evidence that is externally accessible. When possible, use these views
instead of `pd-ctl hot history`:

```sql
-- Current hotspot distribution
SELECT
  DB_NAME, TABLE_NAME, INDEX_NAME, TYPE, REGION_ID, MAX_HOT_DEGREE, FLOW_BYTES
FROM INFORMATION_SCHEMA.TIDB_HOT_REGIONS
ORDER BY FLOW_BYTES DESC
LIMIT 50;

-- Hotspot history in recent window
SELECT
  UPDATE_TIME, DB_NAME, TABLE_NAME, INDEX_NAME, REGION_ID, STORE_ID,
  TYPE, HOT_DEGREE, FLOW_BYTES, KEY_RATE, QUERY_RATE
FROM INFORMATION_SCHEMA.TIDB_HOT_REGIONS_HISTORY
WHERE UPDATE_TIME >= NOW() - INTERVAL 30 MINUTE
ORDER BY UPDATE_TIME DESC
LIMIT 500;
```

## 7) Step 5: `pd-ctl` Fallback (Internal Access Only)

Use only when SQL/monitoring/log evidence is insufficient and cluster access is
available:

```bash
# Hotspot views
pd-ctl -u http://<pd>:2379 hot read
pd-ctl -u http://<pd>:2379 hot write
pd-ctl -u http://<pd>:2379 hot store
pd-ctl -u http://<pd>:2379 hot buckets <region_id>

# Top Region and execution status
pd-ctl -u http://<pd>:2379 region topread query 20
pd-ctl -u http://<pd>:2379 region topwrite byte 20
pd-ctl -u http://<pd>:2379 region topcpu 20
pd-ctl -u http://<pd>:2379 operator show
pd-ctl -u http://<pd>:2379 operator check <region_id>
```

## 8) Decision Tree (Symptom -> Diagnosis -> Action)

### A. `hot read/write` is clearly hot, but hotspot balancing is ineffective

Diagnosis:

- whether `balance-hot-region-scheduler` exists and is paused/disabled;
- whether `hot-region-schedule-limit` is too low (default: 4);
- whether operators are timing out or heavily backlogged;
- whether a super-hot region exists (heat follows region migration).

Actions:

- restore/enable hotspot scheduler first and ensure it is not paused;
- tune in small steps in controlled windows (one parameter per round);
- if operator execution is slow, prioritize snapshot/network/disk execution bottlenecks;
- for super-hot regions, locate with Top SQL / Slow SQL / SQL hotspot views and
  prioritize workload-side scatter.

If still ineffective, branch by hotspot type:

- Write hotspot scenarios (hot small table / hot index / append write):
  - hot small table or hot index: prioritize table/index scatter;
  - append write: evaluate write-dispersion options like `SHARD_ROW_ID_BITS`.
- Read hotspot scenarios:
  - hot small table/index still prioritize scatter;
  - combine threshold tuning with `load-base-split` capability.

### B. TiKV has QPS or CPU hotspots, but `hot read/write` is not obvious

Common causes:

- many small hotspots diluted/filtered in reporting and aggregation;
- low bytes/keys but high query, making PD hotspot detection less sensitive;
- read hotspots dominated by scheduler-layer traffic and other pressure, rather than pure coprocessor traffic;
- version-specific metric blind spots (historical mvcc/tombstone-related cases).

Actions:

- prioritize structural scatter by locating hotspot tables/indexes via Top SQL / Slow SQL / SQL hotspot views;
- if tombstone accumulation likely causes CPU skew, evaluate compact in low-traffic windows.

Emergency fallback only when all are true:

- no clear hotspot range;
- Top SQL unavailable;
- small hotspots are uniformly distributed.

- for read hotspots, consider temporarily adding and later removing `evict-leader-scheduler`;
- for write hotspots, evaluate offline/online reshuffle;
- both require explicit risk notes and rollback plans.

### C. Write hotspot after truncate or rapid post-DDL writes

Diagnosis:

- missing pre-split / scatter;
- TiDB logs include pre-split failures and `context deadline exceeded`.

Actions:

- restore pre-split and scatter workflow;
- tune `tidb_wait_split_region_timeout` when needed;
- for large-table range scatter, evaluate conflict risk with balance scheduling.

### D. Hotspot scheduling conflict or oscillation (mixed read/write high load)

Diagnosis:

- redundant scheduling, repeated migration, and latency jitter;
- over-aggressive parameters causing over-scheduling.

Actions:

- increase `src-tolerance-ratio` / `dst-tolerance-ratio` to reduce over-scheduling;
- decrease `hot-region-schedule-limit` to cap concurrent hotspot operators;
- increase `hot-region-cache-hits-threshold` to filter transient noise;
- observe at least one full peak/valley cycle after each tuning round.

## 9) Tuning Recommendations (Record Before Change)

Record current values for rollback:

```bash
pd-ctl -u http://<pd>:2379 scheduler config balance-hot-region-scheduler show
pd-ctl -u http://<pd>:2379 config show schedule
```

Small-step tuning examples (as needed):

```bash
pd-ctl -u http://<pd>:2379 scheduler config balance-hot-region-scheduler set src-tolerance-ratio 1.1
pd-ctl -u http://<pd>:2379 scheduler config balance-hot-region-scheduler set dst-tolerance-ratio 1.1
pd-ctl -u http://<pd>:2379 config set hot-region-schedule-limit 2
pd-ctl -u http://<pd>:2379 config set hot-region-cache-hits-threshold 5
```

Rollback rules:

- restore old values one by one, do not change many items at once;
- rollback immediately if side effects grow (latency jitter, operator timeout, etc.).

## 10) Output Template

1. Symptom summary: hotspot type, start time, and impact scope;
2. Key evidence: metrics/logs/SQL evidence and critical fields (store/region/operator);
3. Diagnosis: primary and secondary causes with confidence;
4. Immediate mitigation: low-risk and rollback-safe actions with expected watch metrics;
5. Mid-term suggestions: schema/workload/capacity/version optimization;
6. Rollback plan: trigger conditions and exact rollback commands.
