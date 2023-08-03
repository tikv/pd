// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	pdAddr = flag.String("pd", "127.0.0.1:2379", "pd address")

	// GetRegion qps
	regionQps = flag.Int("qps-get-region", 0, "GetRegion qps")
	// ScanRegions qps
	regionsQps = flag.Int("qps-scan-regions", 0, "ScanRegions qps")
	// /stats/region qps
	regionStatsQps = flag.Int("qps-region-stats", 0, "/stats/region qps")
	// ScanRegions the number of region
	regionsSample = flag.Int("regions-sample", 10000, "ScanRegions the number of region")
	// the number of regions
	regionNum = flag.Int("region-num", 1000000, "the number of regions")
	// GetStore qps
	storeQps = flag.Int("qps-store", 0, "GetStore qps")
	// GetStores qps
	storesQps = flag.Int("qps-stores", 0, "GetStores qps")
	// store max id
	maxStoreID = flag.Int("max-store", 100, "store max id")
	// concurrency
	concurrency = flag.Int("concurrency", 1, "client number")
	// brust
	brust = flag.Int("brust", 1, "brust request")
)

var base int = int(time.Second) / int(time.Microsecond)

var clusterID uint64

func main() {
	log.SetFlags(0)
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()
	if *concurrency == 0 {
		log.Println("concurrency == 0, exit")
		return
	}
	if *brust == 0 {
		log.Println("brust == 0, exit")
		return
	}
	pdClis := make([]pdpb.PDClient, 0)
	for i := 0; i < *concurrency; i++ {
		pdClis = append(pdClis, newClient())
	}
	httpClis := make([]*http.Client, 0)
	for i := 0; i < *concurrency; i++ {
		httpClis = append(httpClis, &http.Client{})
	}
	initClusterID(ctx, pdClis[0])

	go handleGetRegion(ctx, pdClis)
	go handleScanRegions(ctx, pdClis)
	go handleRegionsStats(ctx, httpClis)
	go handleGetStore(ctx, pdClis)
	go handleGetStores(ctx, pdClis)

	<-ctx.Done()
	log.Println("Exit")
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func handleGetRegion(ctx context.Context, pdClis []pdpb.PDClient) {
	g := func(id int, keyLen int) []byte {
		k := make([]byte, keyLen)
		copy(k, fmt.Sprintf("%010d", id))
		return k
	}
	if *regionQps == 0 {
		log.Println("handleGetRegion qps = 0, exit")
		return
	}
	tt := base / *regionQps * *concurrency * *brust
	for _, pdCli := range pdClis {
		go func(pdCli pdpb.PDClient) {
			var ticker = time.NewTicker(time.Duration(tt) * time.Microsecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					id := rand.Intn(*regionNum)*4 + 1
					req := &pdpb.GetRegionRequest{
						Header: &pdpb.RequestHeader{
							ClusterId: clusterID,
						},
						RegionKey: g(id, 56),
					}
					for i := 0; i < *brust; i++ {
						_, err := pdCli.GetRegion(ctx, req)
						if err != nil {
							log.Println(err)
						}
					}
				case <-ctx.Done():
					log.Println("Got signal to exit handleGetRegion")
					return
				}
			}
		}(pdCli)
	}
}

func handleScanRegions(ctx context.Context, pdClis []pdpb.PDClient) {
	g := func(id int, keyLen int) []byte {
		k := make([]byte, keyLen)
		copy(k, fmt.Sprintf("%010d", id))
		return k
	}
	if *regionsQps == 0 {
		log.Println("handleScanRegions qps = 0, exit")
		return
	}
	tt := base / *regionsQps * *concurrency * *brust
	for _, pdCli := range pdClis {
		go func(pdCli pdpb.PDClient) {
			var ticker = time.NewTicker(time.Duration(tt) * time.Microsecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					upperBound := *regionNum / *regionsSample
					random := rand.Intn(upperBound)
					startID := *regionsSample*random*4 + 1
					endID := *regionsSample*(random+1)*4 + 1
					req := &pdpb.ScanRegionsRequest{
						Header: &pdpb.RequestHeader{
							ClusterId: clusterID,
						},
						Limit:    int32(*regionsSample),
						StartKey: g(startID, 56),
						EndKey:   g(endID, 56),
					}
					for i := 0; i < *brust; i++ {
						_, err := pdCli.ScanRegions(ctx, req)
						if err != nil {
							log.Println(err)
						}
					}
				case <-ctx.Done():
					log.Println("Got signal to exit handleScanRegions")
					return
				}
			}
		}(pdCli)
	}
}

func handleRegionsStats(ctx context.Context, httpClis []*http.Client) {
	g := func(id int, keyLen int) []byte {
		k := make([]byte, keyLen)
		copy(k, fmt.Sprintf("%010d", id))
		return k
	}
	if *regionStatsQps == 0 {
		log.Println("handleRegionsStats qps = 0, exit")
		return
	}
	tt := base / *regionStatsQps * *concurrency * *brust
	for _, pdCli := range httpClis {
		go func(pdCli *http.Client) {
			var ticker = time.NewTicker(time.Duration(tt) * time.Microsecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					upperBound := *regionNum / *regionsSample
					random := rand.Intn(upperBound)
					startID := *regionsSample*random*4 + 1
					endID := *regionsSample*(random+1)*4 + 1
					for i := 0; i < *brust; i++ {
						req, _ := http.NewRequest(http.MethodGet, "http://"+*pdAddr+fmt.Sprintf("/pd/api/v1/stats/region?start_key=%s&end_key=%s&%s",
							url.QueryEscape(string(g(startID, 56))),
							url.QueryEscape(string(g(endID, 56))),
							"",
						), nil)
						res, err := pdCli.Do(req)
						if err != nil {
							log.Println(err)
						}
						res.Body.Close()
					}
				case <-ctx.Done():
					log.Println("Got signal to exit handleScanRegions")
					return
				}
			}
		}(pdCli)
	}
}

func handleGetStore(ctx context.Context, pdClis []pdpb.PDClient) {
	if *storeQps == 0 {
		log.Println("handleGetStore qps = 0, exit")
		return
	}
	tt := base / *storeQps * *concurrency * *brust
	for _, pdCli := range pdClis {
		go func(pdCli pdpb.PDClient) {
			var ticker = time.NewTicker(time.Duration(tt) * time.Microsecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					req := &pdpb.GetStoreRequest{
						Header: &pdpb.RequestHeader{
							ClusterId: clusterID,
						},
					}
					storeID := rand.Intn(*maxStoreID) + 1
					req.StoreId = uint64(storeID)
					for i := 0; i < *brust; i++ {
						_, err := pdCli.GetStore(ctx, req)
						if err != nil {
							log.Println(err)
						}
					}
				case <-ctx.Done():
					log.Println("Got signal to exit handleGetStore")
					return
				}
			}
		}(pdCli)
	}
}

func handleGetStores(ctx context.Context, pdClis []pdpb.PDClient) {
	if *storesQps == 0 {
		log.Println("handleGetStores qps = 0, exit")
		return
	}
	tt := base / *storesQps * *concurrency * *brust
	for _, pdCli := range pdClis {
		go func(pdCli pdpb.PDClient) {
			var ticker = time.NewTicker(time.Duration(tt) * time.Microsecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					req := &pdpb.GetAllStoresRequest{
						Header: &pdpb.RequestHeader{
							ClusterId: clusterID,
						},
					}
					for i := 0; i < *brust; i++ {
						_, err := pdCli.GetAllStores(ctx, req)
						if err != nil {
							log.Println(err)
						}
					}
				case <-ctx.Done():
					log.Println("Got signal to exit handleGetStores")
					return
				}
			}
		}(pdCli)
	}
}

func exit(code int) {
	os.Exit(code)
}

func newClient() pdpb.PDClient {
	addr := trimHTTPPrefix(*pdAddr)
	cc, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal("failed to create gRPC connection", zap.Error(err))
	}
	return pdpb.NewPDClient(cc)
}

func trimHTTPPrefix(str string) string {
	str = strings.TrimPrefix(str, "http://")
	str = strings.TrimPrefix(str, "https://")
	return str
}

func initClusterID(ctx context.Context, cli pdpb.PDClient) {
	cctx, cancel := context.WithCancel(ctx)
	res, err := cli.GetMembers(cctx, &pdpb.GetMembersRequest{})
	cancel()
	if err != nil {
		log.Fatal("failed to get members", zap.Error(err))
	}
	if res.GetHeader().GetError() != nil {
		log.Fatal("failed to get members", zap.String("err", res.GetHeader().GetError().String()))
	}
	clusterID = res.GetHeader().GetClusterId()
}
