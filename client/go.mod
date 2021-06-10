module github.com/tikv/pd/client

go 1.16

require (
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/kvproto v0.0.0-20210527074428-73468940541b
	github.com/pingcap/log v0.0.0-20210317133921-96f4fcab92a4
	github.com/prometheus/client_golang v1.1.0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/goleak v1.1.10
	go.uber.org/zap v1.15.0
	google.golang.org/grpc v1.26.0
)

// Fix panic in unit test with go >= 1.14, ref: etcd-io/bbolt#201 https://github.com/etcd-io/bbolt/pull/201
replace go.etcd.io/bbolt => go.etcd.io/bbolt v1.3.5
