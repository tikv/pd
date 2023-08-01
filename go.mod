module github.com/tikv/pd

go 1.12

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/chzyer/logex v1.1.10 // indirect
	github.com/chzyer/readline v0.0.0-20171208011716-f6d7a1f6fbf3
	github.com/chzyer/test v0.0.0-20180213035817-a1ea475d72b1 // indirect
	github.com/coreos/go-semver v0.2.0
	github.com/coreos/pkg v0.0.0-20160727233714-3ac0863d7acf
	github.com/docker/go-units v0.4.0
	github.com/dustin/go-humanize v0.0.0-20180421182945-02af3965c54e // indirect
	github.com/eknkc/amber v0.0.0-20171010120322-cdade1c07385 // indirect
	github.com/ghodss/yaml v1.0.1-0.20190212211648-25d852aebe32
	github.com/gogo/protobuf v1.2.1
	github.com/golang/groupcache v0.0.0-20181024230925-c65c006176ff // indirect
	github.com/golang/protobuf v1.3.2
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/google/btree v1.0.0
	github.com/gorilla/context v0.0.0-20160226214623-1ea25387ff6f // indirect
	github.com/gorilla/mux v1.6.1
	github.com/gorilla/websocket v1.2.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/juju/ratelimit v1.0.1
	github.com/mattn/go-shellwords v1.0.3
	github.com/montanaflynn/stats v0.0.0-20151014174947-eeaced052adb
	github.com/onsi/gomega v1.4.2 // indirect
	github.com/opentracing/opentracing-go v1.0.2
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errcode v0.0.0-20180921232412-a1a7271709d9
	github.com/pingcap/failpoint v0.0.0-20190512135322-30cc7431d99c
	github.com/pingcap/kvproto v0.0.0-20190516013202-4cf58ad90b6c
	github.com/pingcap/log v0.0.0-20190715063458-479153f07ebd
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.0.0
	github.com/sirupsen/logrus v1.2.0
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.1
	github.com/syndtr/goleveldb v0.0.0-20180815032940-ae2bd5eed72d
	github.com/tmc/grpc-websocket-proxy v0.0.0-20171017195756-830351dc03c6 // indirect
	github.com/unrolled/render v0.0.0-20171102162132-65450fb6b2d3
	github.com/urfave/negroni v0.3.0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.10.0
	google.golang.org/grpc v1.23.1
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

replace google.golang.org/grpcgrpc => github.com/HuSharp/grpc-go v0.0.0-20230731085747-8a15bcfb3a9d
