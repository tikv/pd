# pd-add-preparing-stores

This is a test-only tool for an isolated PD test cluster. It registers a range
of mock stores through the PD gRPC API with `NodeState_Preparing` so that
Preparing-progress behavior can be measured. It does not start TiKV processes,
send store heartbeats, or create usable replicas.

The tool first reads the cluster ID and all existing stores. If any requested
store ID already exists, it stops before issuing `PutStore`, avoiding a partial
registration caused by an ID collision. Use an unused ID range and never point
it at a production or shared PD cluster.

Build from the `pd/tools` module:

```bash
GOOS=linux GOARCH=amd64 go build -o pd-add-preparing-stores ./pd-add-preparing-stores
```

Run it with a PD client address, the first mock store ID, and the number of
stores to add:

```bash
./pd-add-preparing-stores http://127.0.0.1:2379 1000 10
```

The operation has a 30-second context deadline. The mock address and version
are intentionally fixed in the source because this tool is only for the
isolated benchmark setup. A store remains in the Preparing lifecycle only as
long as the target PD's scheduling/state logic keeps it there; this tool does
not emulate the TiKV side of the lifecycle.

Compile the tool against the same PD/client dependency family as the target
cluster. Older release branches may use a different `grpcutil` import path.
