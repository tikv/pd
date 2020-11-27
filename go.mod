module github.com/tikv/pd

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751
	github.com/aws/aws-sdk-go v1.35.3
	github.com/cakturk/go-netstat v0.0.0-20200220111822-e5b49efee7a5
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/coreos/go-semver v0.3.0
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f
	github.com/docker/go-units v0.4.0
	github.com/go-echarts/go-echarts v1.0.0
	github.com/go-playground/overalls v0.0.0-20180201144345-22ec1a223b7c
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.4
	github.com/google/btree v1.0.0
	github.com/gorilla/mux v1.7.4
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/juju/ratelimit v1.0.1
	github.com/mattn/go-shellwords v1.0.3
	github.com/mgechev/revive v1.0.2
	github.com/montanaflynn/stats v0.5.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/phf/go-queue v0.0.0-20170504031614-9abe38d0371d
	github.com/pingcap-incubator/tidb-dashboard v0.0.0-20201126111827-6c8be2240067
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errcode v0.3.0
	github.com/pingcap/errors v0.11.5-0.20201029093017-5a7df2af2ac7
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/kvproto v0.0.0-20201113092725-08f2872278eb
	github.com/pingcap/log v0.0.0-20201112100606-8f1e84a3abc8
	github.com/pingcap/sysutil v0.0.0-20201021075216-f93ced2829e2
	github.com/pingcap/tiup v1.2.3
	github.com/prometheus/client_golang v1.2.1
	github.com/prometheus/common v0.9.1
	github.com/sasha-s/go-deadlock v0.2.0
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/swaggo/http-swagger v0.0.0-20200308142732-58ac5e232fba
	github.com/swaggo/swag v1.6.6-0.20200529100950-7c765ddd0476
	github.com/syndtr/goleveldb v1.0.1-0.20190318030020-c3a204f8e965
	github.com/unrolled/render v1.0.1
	github.com/urfave/negroni v0.3.0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/goleak v0.10.0
	go.uber.org/zap v1.15.0
	golang.org/x/tools v0.0.0-20200527183253-8e7acdbce89d
	google.golang.org/grpc v1.26.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

replace (
	github.com/sirupsen/logrus => github.com/sirupsen/logrus v1.2.0
	go.etcd.io/bbolt => go.etcd.io/bbolt v1.3.5
)
