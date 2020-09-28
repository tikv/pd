module github.com/tikv/pd

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751
	github.com/cakturk/go-netstat v0.0.0-20200220111822-e5b49efee7a5
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/coreos/go-semver v0.2.0
	github.com/coreos/pkg v0.0.0-20160727233714-3ac0863d7acf
	github.com/docker/go-units v0.4.0
	github.com/eknkc/amber v0.0.0-20171010120322-cdade1c07385 // indirect
	github.com/go-echarts/go-echarts v1.0.0
	github.com/go-playground/overalls v0.0.0-20180201144345-22ec1a223b7c
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.4
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/btree v1.0.0
	github.com/gorilla/mux v1.7.3
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/juju/ratelimit v1.0.1
	github.com/mattn/go-shellwords v1.0.3
	github.com/mgechev/revive v1.0.2
	github.com/montanaflynn/stats v0.0.0-20151014174947-eeaced052adb
	github.com/onsi/gomega v1.4.2 // indirect
	github.com/opentracing/opentracing-go v1.0.2
	github.com/petermattis/goid v0.0.0-20180202154549-b0b1615b78e5 // indirect
	github.com/phf/go-queue v0.0.0-20170504031614-9abe38d0371d
	github.com/pingcap-incubator/tidb-dashboard v0.0.0-20200921100341-0e148dfc0029
	github.com/pingcap/check v0.0.0-20191216031241-8a5a85928f12
	github.com/pingcap/errcode v0.0.0-20180921232412-a1a7271709d9
	github.com/pingcap/errors v0.11.5-0.20200917111840-a15ef68f753d
	github.com/pingcap/failpoint v0.0.0-20191029060244-12f4ac2fd11d
	github.com/pingcap/kvproto v0.0.0-20200927025644-73dc27044686
	github.com/pingcap/log v0.0.0-20200511115504-543df19646ad
	github.com/pingcap/sysutil v0.0.0-20200715082929-4c47bcac246a
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/common v0.4.1
	github.com/sasha-s/go-deadlock v0.2.0
	github.com/sirupsen/logrus v1.2.0
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.1
	github.com/swaggo/http-swagger v0.0.0-20200308142732-58ac5e232fba
	github.com/swaggo/swag v1.6.6-0.20200529100950-7c765ddd0476
	github.com/syndtr/goleveldb v0.0.0-20180815032940-ae2bd5eed72d
	github.com/unrolled/render v0.0.0-20171102162132-65450fb6b2d3
	github.com/urfave/negroni v0.3.0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/goleak v0.10.0
	go.uber.org/zap v1.15.0
	golang.org/x/tools v0.0.0-20200527183253-8e7acdbce89d
	google.golang.org/grpc v1.25.1
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

replace go.etcd.io/bbolt => go.etcd.io/bbolt v1.3.5
