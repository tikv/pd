# pd-ctl

`pd-ctl` is a command line tool for PD, used to obtain the state information of the cluster and tune the cluster.

## Build

1. [Go](https://golang.org/) Version 1.26 or later
2. In the root directory of the [PD project](https://github.com/tikv/pd), use the `make pd-ctl` command to compile and generate `bin/pd-ctl`.

## Usage

The details about how to use `pd-ctl` can be found in [PD Control User Guide](https://docs.pingcap.com/tidb/dev/pd-control).

## GC state troubleshooting

Use `gc-state` to inspect the safe points and barriers that can block GC. The command is read-only and emits deterministic JSON for scripts and diffs.

The default `keyspace` and `all` views request global barriers. They omit barriers that PD returns with a zero TTL because those barriers normally represent expired barriers awaiting lazy deletion. Add `--include-expired` to include those barriers in the existing `gc_barriers` or `global_gc_barriers` array with `ttl_seconds` set to `0`. An empty `global_gc_barriers` array means PD returned no global barriers.

For example, inspect one keyspace and include zero-TTL barriers:

```bash
pd-ctl gc-state keyspace 42 --include-expired
```

Use `keyspace` when diagnosing one GC scope. Inspect a keyspace by its decimal ID:

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
  "gc_barriers": [
    {
      "barrier_id": "br",
      "barrier_ts": 464950000000000000,
      "ttl_seconds": 3600
    }
  ],
  "global_gc_barriers": [
    {
      "barrier_id": "native_br",
      "barrier_ts": 464940000000000000,
      "ttl_seconds": 9223372036854775807
    }
  ]
}
```

The response contains both `requested_keyspace_id` and `effective_keyspace_id`. They are equal for keyspace-level GC. A keyspace that uses unified GC returns `4294967295`, the NullKeyspace ID, as its effective scope. The local and global barriers come from one `GetGCState` read. The same global list applies to every keyspace.

Use `all` only when you need every effective GC scope because it enumerates all keyspaces. Inspect every effective GC scope together with cluster-wide state:

```bash
pd-ctl gc-state all
```

```json
{
  "gc_states": [
    {
      "keyspace_id": 4294967295,
      "is_keyspace_level_gc": false,
      "txn_safe_point": 465000000000000000,
      "gc_safe_point": 464900000000000000,
      "gc_barriers": [
        {
          "barrier_id": "br",
          "barrier_ts": 464950000000000000,
          "ttl_seconds": 3600
        }
      ]
    }
  ],
  "global_gc_barriers": [
    {
      "barrier_id": "native_br",
      "barrier_ts": 464940000000000000,
      "ttl_seconds": 9223372036854775807
    }
  ]
}
```

The combined response sorts effective `gc_states` by `keyspace_id` and reports cluster-wide barriers once in the top-level `global_gc_barriers` array. Unified GC keyspaces share the NullKeyspace scope, so their marker records are not reported as separate states. The real NullKeyspace state appears once with its safe points and local barriers. When no local or global barriers exist, the corresponding arrays are encoded as `[]`. Barrier TTLs use remaining seconds, and `9223372036854775807` means that a barrier never expires. Because PD rounds remaining TTLs down to whole seconds, a zero TTL can also represent a barrier with less than one second remaining.

GC state RPCs time out after 30 seconds by default. The timeout applies to both subcommands and must be a positive duration. If a large cluster takes longer to enumerate, increase it with `--timeout` using Go duration syntax:

```bash
pd-ctl gc-state all --timeout 2m
```

Use `--exclude-global-barriers` to skip the global-barrier read and remove the `global_gc_barriers` field from the JSON output. The flag applies to both subcommands:

```bash
pd-ctl gc-state keyspace 42 --exclude-global-barriers
pd-ctl gc-state all --exclude-global-barriers --include-expired
```

When combined with `--include-expired`, exclusion wins for global barriers, while expired local barriers remain visible.
