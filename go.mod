module github.com/tikv/pd

go 1.16

require (
	github.com/AlekSi/gocov-xml v1.0.0
	github.com/BurntSushi/toml v0.3.1
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751
	github.com/aws/aws-sdk-go v1.35.3
	github.com/axw/gocov v1.0.0
	github.com/cakturk/go-netstat v0.0.0-20200220111822-e5b49efee7a5
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/coreos/go-semver v0.3.0
	github.com/docker/go-units v0.4.0
	github.com/gin-gonic/gin v1.7.4
	github.com/go-echarts/go-echarts v1.0.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.3.4
	github.com/google/btree v1.0.0
	github.com/gorilla/mux v1.7.4
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/joho/godotenv v1.4.0
	github.com/mattn/go-shellwords v1.0.12
	github.com/mgechev/revive v1.0.2
	github.com/montanaflynn/stats v0.5.0
	github.com/petermattis/goid v0.0.0-20211229010228-4d14c490ee36 // indirect
	github.com/phf/go-queue v0.0.0-20170504031614-9abe38d0371d
	github.com/pingcap/check v0.0.0-20211026125417-57bd13f7b5f0
	github.com/pingcap/errcode v0.3.0
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/kvproto v0.0.0-20220510035547-0e2f26c0a46a
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/sysutil v0.0.0-20211208032423-041a72e5860d
	github.com/pingcap/tidb-dashboard v0.0.0-20220613053259-1b8920062bd3
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/common v0.6.0
	github.com/sasha-s/go-deadlock v0.2.0
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/swaggo/http-swagger v0.0.0-20200308142732-58ac5e232fba
	github.com/swaggo/swag v1.8.3
	github.com/syndtr/goleveldb v1.0.1-0.20190318030020-c3a204f8e965
	github.com/tidwall/gjson v1.9.3 // indirect
	github.com/unrolled/render v1.0.1
	github.com/urfave/negroni v0.3.0
	// Fix panic in unit test with go >= 1.14, ref: etcd-io/bbolt#201 https://github.com/etcd-io/bbolt/pull/201
	go.etcd.io/bbolt v1.3.5 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/goleak v1.1.12
	go.uber.org/zap v1.19.1
	golang.org/x/text v0.3.7
	golang.org/x/time v0.0.0-20220224211638-0e9765cccd65
	golang.org/x/tools v0.1.10
	google.golang.org/grpc v1.26.0
	gotest.tools/gotestsum v1.7.0
)

replace github.com/pingcap/kvproto v0.0.0-20220510035547-0e2f26c0a46a => github.com/Connor1996/kvproto v0.0.0-20220623054324-95735359d10e
