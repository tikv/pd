grantLeaderPlugin.so: *.go
	GO111MODULE=on GO_ENABLED=1 go build -race -buildmode=plugin -o grantLeaderPlugin.so *.go

.PHONY : clean

clean:
	rm grantLeaderPlugin.so
