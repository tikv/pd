# pd-heartbeat-bench

`pd-heartbeat-bench` is used to bench heartbeart.

1. You need to deploy a cluster that only contain pd firstly, like `tiup playground nightly --pd 3 --kv 0 --db 0`.
2. Then, execute `pd-heartbeart-bench` and set the pd leader as `--pd-endpoints` 