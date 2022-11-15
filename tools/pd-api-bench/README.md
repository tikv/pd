When test ScanRegions and GetRegion, it's better to set region-num.
When test ScanRegions, it's better to set regions-sample, and not greater than 10000.
``` bash
go run ./main.go --region 10 --region-num 200000 --concurrency 10
go run ./main.go --regions 10 --region-num 200000 --regions-sample 1000 --concurrency 10
```

When test GetStore, it's better to set max-store
``` bash
go run ./main.go --max-store 10 --store 10 --concurrency 10
go run ./main.go --stores 10 --concurrency 10
```