When test ScanRegions and GetRegion, it's better to set region-num.
When test ScanRegions, it's better to set regions-sample, and not greater than 10000.
``` bash
go run ./main.go --qps-get-region 10 --region-num 200000 --concurrency 10
go run ./main.go --qps-scan-regions 10 --region-num 200000 --regions-sample 1000 --concurrency 10
go run ./main.go --qps-region-stats 10 --region-num 200000 --regions-sample 1000 --concurrency 10
```

When test GetStore, it's better to set max-store
``` bash
go run ./main.go --max-store 10 --qps-store 10 --concurrency 10
go run ./main.go --qps-stores 10 --concurrency 10
```