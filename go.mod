module github.com/tikv/pd

go 1.16

require (
	github.com/AlekSi/gocov-xml v1.0.0
	github.com/BurntSushi/toml v0.3.1
	github.com/aws/aws-sdk-go v1.35.3
	github.com/axw/gocov v1.0.0
	github.com/cakturk/go-netstat v0.0.0-20200220111822-e5b49efee7a5
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/coreos/go-semver v0.3.0
	github.com/docker/go-units v0.4.0
	github.com/go-echarts/go-echarts v1.0.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.1
	github.com/gorilla/mux v1.7.4
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/influxdata/tdigest v0.0.1
	github.com/juju/ratelimit v1.0.1
	github.com/mattn/go-shellwords v1.0.12
	github.com/mgechev/revive v1.0.2
	github.com/montanaflynn/stats v0.5.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/petermattis/goid v0.0.0-20211229010228-4d14c490ee36 // indirect
	github.com/phf/go-queue v0.0.0-20170504031614-9abe38d0371d
	github.com/pingcap/check v0.0.0-20211026125417-57bd13f7b5f0
	github.com/pingcap/errcode v0.3.0
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/kvproto v0.0.0-20211213085605-3329b3c5404c
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/sysutil v0.0.0-20211208032423-041a72e5860d
	github.com/pingcap/tidb-dashboard v0.0.0-20220117082709-e8076b5c79ba
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.26.0
	github.com/sasha-s/go-deadlock v0.2.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/swaggo/http-swagger v0.0.0-20200308142732-58ac5e232fba
	github.com/swaggo/swag v1.6.6-0.20200529100950-7c765ddd0476
	github.com/syndtr/goleveldb v1.0.1-0.20190318030020-c3a204f8e965
	github.com/unrolled/render v1.0.1
	github.com/urfave/negroni v0.3.0
	go.etcd.io/etcd/api/v3 v3.5.2
	go.etcd.io/etcd/client/pkg/v3 v3.5.2
	go.etcd.io/etcd/client/v3 v3.5.2
	go.etcd.io/etcd/pkg/v3 v3.5.2
	go.etcd.io/etcd/server/v3 v3.5.2
	go.uber.org/goleak v1.1.12
	go.uber.org/zap v1.19.1
	golang.org/x/tools v0.1.5
	google.golang.org/grpc v1.38.0
	gotest.tools/gotestsum v1.7.0
)

replace github.com/prometheus/client_golang => github.com/prometheus/client_golang v1.1.0

replace github.com/prometheus/common => github.com/prometheus/common v0.6.0

// Fix panic in unit test with go >= 1.14, ref: etcd-io/bbolt#201 https://github.com/etcd-io/bbolt/pull/201
replace go.etcd.io/bbolt => go.etcd.io/bbolt v1.3.5

replace github.com/pingcap/tidb-dashboard => github.com/killzoner/tidb-dashboard v0.0.0-20220210152730-5fb3e7522db3
