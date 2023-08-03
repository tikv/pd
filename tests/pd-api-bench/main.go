// Copyright 2023 TiKV Project Authors.
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
	"log"
	"math/rand"
	"net/http"
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

	// // tso client number
	// tsoClientNumber = flag.Int("tso-client-num", 1, "tso client number")
	// // tso qps
	// tsoQPS = flag.Int("tso-qps", 1, "tso qps")
	// // tso concurrency
	// tsoConcurrency = flag.Int("tso-concurrency", 1, "tso concurrency")
	// // tso option

	// GetRegion qps
	region = flag.Int("region", 0, "GetRegion qps")
	// ScanRegions qps
	regions     = flag.Int("regions", 0, "ScanRegions qps")
	regionStats = flag.Int("region-stats", 0, "/stats/region qps")
	// ScanRegions the number of region
	regionsSample = flag.Int("regions-sample", 10000, "ScanRegions the number of region")
	// the number of regions
	regionNum = flag.Int("region-num", 1000000, "the number of regions")
	// GetStore qps
	store = flag.Int("store", 0, "GetStore qps")
	// GetStores qps
	stores = flag.Int("stores", 0, "GetStores qps")
	// store max id
	maxStoreID = flag.Int("max-store", 100, "store max id")
	// concurrency
	concurrency = flag.Int("concurrency", 1, "client number")
	// brust
	brust = flag.Int("brust", 1, "brust request")
)

var base = int(time.Second) / int(time.Microsecond)

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
	if *region == 0 {
		log.Println("handleGetRegion qps = 0, exit")
		return
	}
	tt := base / *region * *concurrency * *brust
	for _, pdCli := range pdClis {
		go func(pdCli pdpb.PDClient) {
			var ticker = time.NewTicker(time.Duration(tt) * time.Microsecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					id := rand.Intn(*regionNum)*4 + 1
					req := &pdpb.GetMinResolvedTimestampRequest{
						Header: &pdpb.RequestHeader{
							ClusterId: clusterID,
						},
					}
					for i := 0; i < *brust; i++ {
						_, err := pdCli.GetMinResolvedTimestamp(ctx, req)
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
