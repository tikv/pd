evictLeaderPlugin.so: *.go
	GO111MODULE=on GO_ENABLED=1 go build  -buildmode=plugin -o evictLeaderPlugin.so *.go

.PHONY : clean

clean:
	rm evictLeaderPlugin.so
