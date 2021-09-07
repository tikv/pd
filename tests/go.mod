module github.com/tikv/pd/tests

go 1.16

require (
	github.com/coreos/go-semver v0.3.0
	github.com/gogo/protobuf v1.3.1
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/kvproto v0.0.0-20210819164333-bd5706b9d9f2
	github.com/pingcap/log v0.0.0-20210625125904-98ed8e2eb1c7
	github.com/spf13/cobra v1.0.0
	github.com/tikv/pd v0.0.0-00010101000000-000000000000
	github.com/tikv/pd/cmd v0.0.0-00010101000000-000000000000
	github.com/tikv/pd/tools v0.0.0-00010101000000-000000000000
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/goleak v1.1.10
	google.golang.org/grpc v1.26.0
)

replace (
	github.com/tikv/pd => ../
	github.com/tikv/pd/cmd => ../cmd
	github.com/tikv/pd/tools => ../tools
)
