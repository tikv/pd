module github.com/tools/pd-tso-bench

go 1.16

require (
	github.com/influxdata/tdigest v0.0.1
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/log v0.0.0-20211215031037-e024ba4eb0ee
	github.com/prometheus/client_golang v1.11.0
	github.com/tikv/pd/client v0.0.0-00010101000000-000000000000
	go.uber.org/zap v1.20.0
)

replace github.com/tikv/pd/client => ../../client
