moveLeaderPlugin.so: *.go
	GO111MODULE=on GO_ENABLED=1 go build -race -gcflags '' -ldflags '-X "github.com/pingcap/pd/server.PDReleaseVersion=v4.0.0-alpha-54-ge678a1f-dirty" -X "github.com/pingcap/pd/server.PDBuildTS=2019-09-19 06:42:25" -X "github.com/pingcap/pd/server.PDGitHash=e678a1f5c022fed729fb79397fe02b6c9f54ff4a" -X "github.com/pingcap/pd/server.PDGitBranch=master"' -buildmode=plugin -o moveLeaderPlugin.so *.go

.PHONY : clean

clean:
	rm moveLeaderPlugin.so
