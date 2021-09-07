module github.com/tikv/pd/cmd

go 1.16

require (
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/kvproto v0.0.0-20210819164333-bd5706b9d9f2
	github.com/pingcap/log v0.0.0-20210625125904-98ed8e2eb1c7
	github.com/pingcap/tidb-dashboard v0.0.0-20210903143224-ee42b1db0c90
	github.com/swaggo/http-swagger v0.0.0-20200308142732-58ac5e232fba
	github.com/swaggo/swag v1.6.6-0.20200529100950-7c765ddd0476
	github.com/tikv/pd v0.0.0-00010101000000-000000000000
	go.uber.org/zap v1.16.0
)

replace github.com/tikv/pd => ../
