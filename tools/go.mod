module github.com/tikv/pd/tools

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/go-echarts/go-echarts v1.0.0
	github.com/mattn/go-shellwords v1.0.3
	github.com/mgechev/revive v1.0.2
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/kvproto v0.0.0-20210819164333-bd5706b9d9f2
	github.com/pingcap/log v0.0.0-20210625125904-98ed8e2eb1c7
	github.com/prometheus/client_golang v1.1.0
	github.com/sasha-s/go-deadlock v0.2.0
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/swaggo/swag v1.6.6-0.20200529100950-7c765ddd0476
	github.com/tikv/pd v0.0.0-00010101000000-000000000000
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.16.0
	golang.org/x/tools v0.0.0-20210112230658-8b4aab62c064
	google.golang.org/grpc v1.26.0
)

replace github.com/tikv/pd => ../
