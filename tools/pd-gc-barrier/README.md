# pd-gc-barrier

`pd-gc-barrier` is a standalone command-line tool for inspecting, creating, updating, and deleting keyspace GC barriers through PD's gRPC APIs. It operates on an explicitly selected keyspace with keyspace-level GC enabled.

## Build

From the repository root:

```sh
(
  cd tools || exit 1
  CGO_ENABLED=0 go build -tags nextgen -o ../bin/pd-gc-barrier ./pd-gc-barrier
)
./bin/pd-gc-barrier --help
```

Use Go 1.25 or newer for the repository's development environment. Set `GOOS` and `GOARCH` when building for another platform. Run the binary on a machine that can reach the PD endpoints.

## Commands

```text
pd-gc-barrier --pd <endpoints> --keyspace-id <id> show
pd-gc-barrier --pd <endpoints> --keyspace-id <id> set <barrier-id> <tso-or-rfc3339-time> --ttl <duration|never>
pd-gc-barrier --pd <endpoints> --keyspace-id <id> delete <barrier-id>
```

Both `--pd` and `--keyspace-id` are required for every command. The examples below use placeholder values; replace them with the actual endpoints, an existing keyspace ID, and an appropriate target timestamp.

```sh
PD='http://127.0.0.1:2379'
KEYSPACE_ID='123'
BARRIER_ID='example-barrier'
TARGET='<target TSO or RFC3339 time>'
```

### Show GC state

```sh
./bin/pd-gc-barrier --pd "$PD" --keyspace-id "$KEYSPACE_ID" show
```

Returns `keyspace_id`, `txn_safe_point`, `gc_safe_point`, and `barriers`. Barriers are sorted by timestamp, then by ID. Each barrier includes its ID, timestamp, and TTL.

The result includes only barriers in the selected keyspace. Global barriers are not listed. Safe points describe GC boundaries; they do not indicate that physical data reclamation or compaction has completed.

### Create or update a barrier

```sh
./bin/pd-gc-barrier --pd "$PD" --keyspace-id "$KEYSPACE_ID" \
  set "$BARRIER_ID" "$TARGET" --ttl 2h
```

If the ID does not exist in the selected keyspace, `set` creates it. Otherwise, it replaces both its timestamp and TTL. Every call requires both values; to change only one, supply the desired unchanged value for the other.

The target timestamp must be greater than or equal to the current transaction safe point. PD enforces this constraint when it writes the barrier. The API does not provide compare-and-swap or require the new timestamp to be greater than the barrier's previous timestamp. Coordinate writers that share a barrier ID.

Use `never` for a barrier that should remain until explicitly deleted:

```sh
./bin/pd-gc-barrier --pd "$PD" --keyspace-id "$KEYSPACE_ID" \
  set "$BARRIER_ID" "$TARGET" --ttl never
```

A finite TTL must be positive, such as `30m` or `2h`. PD rounds it up to whole seconds and resets the expiration time on each successful `set`. In `show` results, a finite TTL is the remaining lifetime reported at query time; expired entries may remain visible until cleaned up. `never` denotes no expiration.

### Delete a barrier

```sh
./bin/pd-gc-barrier --pd "$PD" --keyspace-id "$KEYSPACE_ID" delete "$BARRIER_ID"
```

Returns the deleted barrier's information. Deleting an ID that does not exist succeeds with `deleted_barrier: null`.

Deleting or expiring a barrier removes its retention constraint. Increasing its timestamp can also permit GC to advance. Neither operation reverses GC that has already occurred. The tool performs the requested operation without automatically installing a replacement barrier.

## Timestamp input

The timestamp argument accepts a positive decimal `uint64` TSO or an RFC3339 date and time with an explicit timezone.

| Input | Example | Interpretation |
| --- | --- | --- |
| Decimal TSO | `262144001` | Preserves the exact physical and logical components |
| Date and time with offset | `2025-10-01T00:10:00+08:00` | Uses the specified UTC offset |
| Date and time with milliseconds | `2025-10-01T00:10:00.123+08:00` | Preserves millisecond precision |
| UTC date and time | `2025-09-30T16:10:00Z` | Equivalent to the offset example above |

Date and time input uses `T` as the separator and requires `Z` or an explicit offset. Date-only input and timestamps such as `2025-10-01 00:10:00` are rejected. The time must be after the Unix epoch and fit within the TSO range.

Use at most three fractional digits. Inputs with more than nine fractional digits may currently be truncated before precision validation. Date and time input sets the TSO's logical counter to zero while preserving its physical date, time, and milliseconds. To retain an existing TSO exactly, copy its complete decimal value.

## Connection options

| Option | Description |
| --- | --- |
| `--pd` | Required. Comma-separated PD endpoints; the client discovers the leader. |
| `--keyspace-id` | Required. An existing keyspace ID in the inclusive range `0..16777215`. |
| `--timeout` | Positive timeout for the entire command, including connection initialization. Defaults to `30s`. |
| `--cacert` | Path to the trusted CA certificate file. |
| `--cert` | Path to the client certificate file. |
| `--key` | Path to the client private key file. |

Keyspace `0` is the `DEFAULT` keyspace. `NullKeyspaceID` (`4294967295`), unified GC, and global barrier management are unsupported. Before each operation, the tool reads GC state and rejects a returned scope that differs from the requested keyspace. The reserved barrier ID `gc_worker` cannot be set or deleted.

For TLS connections, provide the certificate files with the command:

```sh
./bin/pd-gc-barrier --pd 'https://pd.example.com:2379' --keyspace-id "$KEYSPACE_ID" \
  --cacert /path/to/ca.pem --cert /path/to/client.pem --key /path/to/client-key.pem \
  show
```

This version of the PD client requires a client certificate and private key to enable TLS. The tool rejects a CA file supplied without the certificate and key, as well as HTTPS endpoints supplied without the client certificate and key.

## Output and errors

Successful commands write JSON to stdout. Diagnostics go to stderr, and failures return a nonzero exit status. Timestamp objects contain a decimal `tso` string and a UTC `time` string. Keeping the TSO as a string avoids precision loss in JSON consumers that use floating-point numbers.

If `set` or `delete` times out or loses its connection, the write may already have succeeded. Run `show` to inspect the actual state before retrying. Exiting the tool does not delete a barrier or reset its TTL.
