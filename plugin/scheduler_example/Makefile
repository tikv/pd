evictLeaderPlugin.so: *.go
	CGO_ENABLED=1 go build -buildmode=plugin -o evictLeaderPlugin.so *.go

.PHONY : clean

clean:
	rm evictLeaderPlugin.so
