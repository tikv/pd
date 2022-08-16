module github.com/tikv/pd/client

go 1.16

require (
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00
	github.com/pingcap/kvproto v0.0.0-20220805093305-ab1ee4d521ab
	github.com/pingcap/log v0.0.0-20211215031037-e024ba4eb0ee
	github.com/prometheus/client_golang v1.11.0
	github.com/stretchr/testify v1.7.0
	go.uber.org/goleak v1.1.11
	go.uber.org/zap v1.20.0
	google.golang.org/grpc v1.43.0
)
