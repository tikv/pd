# pd-ctl

`pd-ctl` is a command line tool for PD, used to obtain the state information of the cluster and tune the cluster.

## Build

1. [Go](https://golang.org/) Version 1.25 or later
2. In the root directory of the [PD project](https://github.com/tikv/pd), use the `make pd-ctl` command to compile and generate `bin/pd-ctl`.

## Usage

The details about how to use `pd-ctl` can be found in [PD Control User Guide](https://docs.pingcap.com/tidb/dev/pd-control).

## GC state troubleshooting

Use `gc-state` to inspect the safe points and barriers that can block GC. The
command is read-only and emits deterministic JSON for scripts and diffs.

Inspect one keyspace by its decimal ID:

```bash
pd-ctl gc-state keyspace 42
```

```json
{
  "requested_keyspace_id": 42,
  "effective_keyspace_id": 4294967295,
  "is_keyspace_level_gc": false,
  "txn_safe_point": 465000000000000000,
  "gc_safe_point": 464900000000000000,
  "gc_barriers": []
}
```

The response contains both `requested_keyspace_id` and
`effective_keyspace_id`. They are equal for keyspace-level GC. A keyspace that
uses unified GC returns `4294967295`, the NullKeyspace ID, as its effective
scope. The response contains local `gc_barriers` only.

Inspect cluster-wide state when the local barriers do not explain the
effective safe point:

```bash
pd-ctl gc-state global
```

```json
{
  "global_gc_barriers": []
}
```

The global response does not contain per-keyspace safe points or local
barriers. Its current field is `global_gc_barriers`; other cluster-wide GC
state can be added to the same object in the future.

Inspect every effective GC scope together with cluster-wide state:

```bash
pd-ctl gc-state all
```

```json
{
  "gc_states": [
    {
      "keyspace_id": 42,
      "is_keyspace_level_gc": true,
      "txn_safe_point": 465000000000000000,
      "gc_safe_point": 464900000000000000,
      "gc_barriers": []
    }
  ],
  "global_gc_barriers": []
}
```

The combined response sorts effective `gc_states` by `keyspace_id` and reports
cluster-wide barriers once in the top-level `global_gc_barriers` array. Unified
GC keyspaces share the NullKeyspace scope, so their marker records are not
reported as separate states.
Barrier TTLs use remaining seconds, and `9223372036854775807` means that a
barrier never expires.
