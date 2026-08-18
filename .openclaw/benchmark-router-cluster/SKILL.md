---
name: benchmark-router-cluster
description: Controller-style sysbench benchmarking on router/TiDB cluster hosts over SSH with strict round sequencing by thread list, mandatory wait gates, run-only or prepare+run modes, stale session/process checks, and explicit plan+permit before execution. Use when user asks to benchmark with different thread counts, repeated rounds, or long-running router cluster tests.
---

# Benchmark Router Cluster (Controller)

Always operate as a controller. Do not jump straight to execution.

## Mandatory interaction contract

Before any benchmark command:
1. Print execution plan (host, workload, DB args, rounds, wait seconds, prepare/cleanup policy).
2. Ask for explicit permit.
3. Run only after user says permit/approved/run now.

If user changes parameters, reprint plan and request permit again.

## Required requirements

Always require these before execution:
1. SSH remote machine target (`target_host` + `ssh_user`).
2. Per-round `threads` parameter (single `threads` or ordered `thread_list` for controller rounds).

## Inputs

Collect/confirm:
- `target_host` (SSH target)
- `ssh_user`
- fixed `topology_yaml`: `references/tiup-router-cluster-template.yaml` (do not ask user to provide another yaml unless explicitly requested)
- `workload` (`oltp_read_write` by default; optional override: `oltp_point_select`)
- `time` (default `300`), `tables`, `table_size`
- one of:
  - `threads` (single-round), or
  - `thread_list` (multi-round controller; each round must use its own thread value)
- `wait_seconds_between_rounds` (for multi-round)
- `prepare_mode` (`prepare_once` or `skip_prepare`)
- `cleanup_mode` (`skip_cleanup` by default)

Do not require mysql host/port as user input. Resolve TiDB host/port from `tidb_servers` in fixed yaml `references/tiup-router-cluster-template.yaml`.
Default to first TiDB server entry when not specified.
## TiUP topology template

Use this fixed file for router-cluster deploy topology:
- `references/tiup-router-cluster-template.yaml`

Deploy policy:
- Always deploy cluster with this template yaml unless user explicitly overrides.
- Use `tiup-cluster deploy` with `--skip-create-user`.
- Default cluster name: `router`.
- In fresh or unknown environments, perform deploy/start readiness before mysql connectivity/auth preflight.
- Build and package TiDB binary on the remote machine during prepare stage:
  - Target package path: `~/tidb/bin/tidb.tar.gz`.
  - If package exists, mark build/package as `[SKIP]`.
  - Otherwise build TiDB and create package tarball at that path.
- After preflight checks pass (before benchmark run), patch TiDB with the packaged tarball:
  - `tiup-cluster patch router ~/tidb/bin/tidb.tar.gz -R tidb -y`

## Prepare for router cluster (preconditions)

Preconditions output must be step-by-step and explicit:
- Print each step header and action details.
- Print outcome markers for every sub-step: `[OK]` / `[SKIP]` / `[FAIL]`.
- On failure, print the failing step id/name and stop immediately.

When user asks to prepare router cluster environment (or when benchmark environment is not ready), run:

```bash
bash scripts/prepare_router_cluster.sh \
  --target-host "$target_host" \
  --ssh-user "$ssh_user"
```

Prepare behavior must:
1. Verify SSH connectivity.
2. Install runtime tools: `sysbench`, mysql client, and **latest** `golang` (from go.dev release tarball, not distro-pinned old versions).
3. Persist PATH in `~/.bashrc` + `~/.profile` (ensure `sysbench`/`mysql`/`go` are directly runnable, include `/usr/local/go/bin` when present).
4. Verify `tiup`/`tiup-cluster` availability.
5. If `tiup` and `tiup-cluster` already exist, mark precondition as `[SKIP]` for tiup bootstrap.
6. If missing, ensure `~/tiup` repo exists (`git clone https://github.com/pingcap/tiup.git` when absent).
7. If `~/tiup/bin` is empty, run `go mod tidy` first, then run `make tiup cluster` in `~/tiup` to build required binaries (`tiup` + `tiup-cluster`) without full lint gate.
8. After successful build, export `~/tiup/bin` into PATH immediately and persist it in `~/.bashrc` + `~/.profile`.
9. Build TiDB binary and package `~/tidb/bin/tidb.tar.gz` (skip if package already exists).

Do **not** install/configure MinIO in this skill.

## Preflight checks

Run preflight before benchmark:
1. SSH reachable.
2. `sysbench` exists remotely.
3. Parse TiDB host/port from fixed topology yaml `references/tiup-router-cluster-template.yaml` (`tidb_servers`) unless user explicitly overrides.
4. **Cluster readiness gate (must happen before mysql auth check):**
   - Check whether TiDB is reachable at parsed host/port.
   - If unreachable, deploy/start router cluster first (using fixed topology + `--skip-create-user`) and wait until TiDB port is ready.
   - Only continue after readiness is `[OK]`.
5. DB auth check with `mysql` command using parsed host/port.
6. Patch gate: patch `~/tidb/bin/tidb.tar.gz` to cluster tidb before benchmark run.
7. If `skip_prepare`, verify table count/range matches `--tables` expectation.

If mismatch:
- Suggest either adjusting `--tables` to existing table count,
- or using fresh DB and `prepare_once`.

## Execution model

Use strict sequential controller logic.

### Single round
- Optional `prepare_once`.
- Run one benchmark with given `threads`.
- Never run cleanup unless explicitly requested.

### Multi round
- Optional `prepare_once` once before round loop.
- For each round in order:
  1. announce `Round i / N` with current threads
  2. run benchmark
  3. capture round summary (TPS/QPS, avg/p95/max latency, errors)
  4. wait `wait_seconds_between_rounds` (except final round)

Do not skip round index.

## Long-running process handling

Prefer remote background mode for long tests:
- Start via `nohup ... > /tmp/<name>.log 2>&1 &`
- Return PID and log file path.
- Verify process exists with `pgrep -af`.
- If local tool session hangs but remote process is gone, report clearly and offer restart.

If user asks to stop:
- kill the tracked process/session first,
- confirm stopped,
- then (only if asked) start a new run.

## Error policy

Handle and report clearly:
- auth failure (`Access denied`)
- table exists on prepare (1050)
- missing tables on run (`sbtestNNN doesn't exist`)
- TiDB retryable transaction errors (e.g., 8022 / TxnLockNotFound)

When failing, propose one concrete next action and wait for user confirmation.

## Output format

For each completed round report:
- Threads
- TPS/QPS (final)
- Latency avg/p95/max
- Errors/Reconnects

For multi-round, append final comparison table ordered by round/thread.
