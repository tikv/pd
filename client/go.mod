module github.com/tikv/pd/client

go 1.16

require (
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00
	github.com/pingcap/kvproto v0.0.0-20230112063313-a14c44ef44b3
	github.com/pingcap/log v1.1.1-0.20221110025148-ca232912c9f3
	github.com/prometheus/client_golang v1.11.0
	github.com/stretchr/testify v1.8.1
	go.uber.org/goleak v1.1.11
	go.uber.org/zap v1.20.0
	google.golang.org/grpc v1.51.0
)
