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
2. **Key Visualizer heatmap**
3. **Top SQL and Slow SQL**
4. **`pd-ctl` hotspot and history data (fallback and evidence supplement)**

Do not start with `pd-ctl` directly. Use monitoring first to determine whether the system can detect hotspots, schedule hotspots, and execute operators.

## 3) Step 1: PD / TiKV Monitoring Investigation (Primary Path)

### 3.1 Capture PD configuration snapshot first

```bash
pd-ctl -u http://<pd>:2379 scheduler show
pd-ctl -u http://<pd>:2379 scheduler show --status paused
pd-ctl -u http://<pd>:2379 scheduler config balance-hot-region-scheduler show
pd-ctl -u http://<pd>:2379 config show schedule
pd-ctl -u http://<pd>:2379 config show all | grep halt
```

### 3.2 PD monitoring (panel order)

Start with **Region health**:

- Check pending peers / down peers / offline peers / extra peers / missing peers.
- If region health is abnormal, fix health issues before hotspot scheduling analysis.

Then verify scheduler runtime:

- **Scheduler is running**: confirm `balance-hot-region-scheduler` keeps running.
- **Schedulers reach limit**: check whether scheduling is frequently blocked by limits.

Then check operator lifecycle:

- **Schedule operator create / check / finish / timeout / replaced or canceled**.
- Key signal: normal creation but low finish with high timeout usually indicates execution bottlenecks, not scheduling logic bottlenecks.

Finally check hotspot scheduling metrics:

- **Statistics - hot write / Statistics - hot read**.
- Focus on:
  - whether hot region leader/peer distribution is overly concentrated;
  - whether store-level read/write bytes/keys/query are imbalanced;
  - whether **Selector write events / Selector read events** are abnormal;
  - whether **Direction of hotspot transfer leader / move peer** shows repeated moves without real dispersion.

### 3.3 TiKV monitoring (aligned with PD conclusions)

CPU path:

- First check store-level CPU skew.
- Then inspect thread CPU:
  - `Scheduler worker CPU` (often related to write hotspots);
  - `Unified read pool CPU` (often related to read hotspots);
  - `gRPC poll CPU` (may relate to both read and write hotspots).

Traffic path:

- Check whether MBps trends align with PD hotspot conclusions.
- If PD shows scheduling activity but TiKV shows no improvement, prioritize snapshot/disk/network execution bottlenecks.

## 4) Step 2: Key Visualizer Heatmap

Use this to identify hotspot shapes and business root-cause hints:

- Read hotspot:
  - isolated bright block: commonly a hot small table;
  - isolated bright column: commonly a hot index;
  - trapezoid/triangle bright area: commonly linear range scan.
- Write hotspot:
  - continuously upward diagonal line: commonly append-write pattern.

## 5) Step 3: Top SQL / Slow SQL

If Dashboard is available, this step is mandatory:

- Identify hotspot SQL, hotspot tables, and hotspot indexes.
- Cross-validate with Key Visualizer ranges and PD/TiKV metrics.
- Build a causal chain from request pattern to hotspot distribution.

## 6) Step 4: `pd-ctl` Hotspot and History Data (Fallback)

Use this only when monitoring evidence is insufficient or needs supplementation:

```bash
# Hotspot views
pd-ctl -u http://<pd>:2379 hot read
pd-ctl -u http://<pd>:2379 hot write
pd-ctl -u http://<pd>:2379 hot store
pd-ctl -u http://<pd>:2379 hot buckets <region_id>

# History hotspot (millisecond timestamps)
# Optional filter keys: hot_region_type, region_id, store_id, peer_id, is_leader, is_learner
pd-ctl -u http://<pd>:2379 hot history <start_ms> <end_ms> hot_region_type write store_id 1,2

# Top Region and execution status
pd-ctl -u http://<pd>:2379 region topread query 20
pd-ctl -u http://<pd>:2379 region topwrite byte 20
pd-ctl -u http://<pd>:2379 region topcpu 20
```

## 7) Decision Tree (Symptom -> Diagnosis -> Action)

### A. `hot read/write` is clearly hot, but hotspot balancing is ineffective

Diagnosis:

- whether `balance-hot-region-scheduler` exists and is paused/disabled;
- whether `hot-region-schedule-limit` is too low (default: 4);
- whether operators are timing out or heavily backlogged;
- whether a super-hot region exists (heat follows region migration).

Actions:

- restore/enable hotspot scheduler first and ensure it is not paused;
- tune in small steps in controlled windows (one parameter per round);
- if operator execution is slow, prioritize TiKV bottlenecks;
- for super-hot regions, locate with Key Visualizer + Top SQL + Slow SQL and prioritize workload-side scatter.

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

- prioritize structural scatter by locating hotspot tables/indexes via Top SQL / Slow SQL;
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

## 8) Tuning Recommendations (Record Before Change)

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

## 9) Output Template

1. Symptom summary: hotspot type, start time, and impact scope;
2. Key evidence: commands and critical fields (store/region/operator);
3. Diagnosis: primary and secondary causes with confidence;
4. Immediate mitigation: low-risk and rollback-safe actions with expected watch metrics;
5. Mid-term suggestions: schema/workload/capacity/version optimization;
6. Rollback plan: trigger conditions and exact rollback commands.
