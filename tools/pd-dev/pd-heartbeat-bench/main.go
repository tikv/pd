// Copyright 2019 TiKV Project Authors.
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

package heartbeatbench

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	pdHttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/tools/pd-dev/util"
	"go.uber.org/zap"
)

func Run(ctx context.Context) {
	defer logutil.LogPanic()

	rand.New(rand.NewSource(0)) // Ensure consistent behavior multiple times
	statistics.Denoising = false

	cfg := newConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case pflag.ErrHelp:
		os.Exit(0)
	default:
		log.Fatal("parse cmd flags error", zap.Error(err))
	}

	maxVersion = cfg.InitEpochVer
	options := newOptions(cfg)
	// let PD have enough time to start
	time.Sleep(5 * time.Second)

	cli, err := newClient(ctx, cfg)
	if err != nil {
		log.Fatal("create client error", zap.Error(err))
	}

	initClusterID(ctx, cli)
	srv := runHTTPServer(cfg, options)
	regions := new(Regions)
	regions.init(cfg)
	log.Info("finish init regions")
	stores := newStores(cfg.StoreCount)
	stores.update(regions)
	bootstrap(ctx, cli)
	putStores(ctx, cfg, cli, stores)
	log.Info("finish put stores")
	clis := make(map[uint64]pdpb.PDClient, cfg.StoreCount)
	httpCli := pdHttp.NewClient("tools-heartbeat-bench", []string{cfg.PDAddrs}, pdHttp.WithTLSConfig(util.LoadTLSConfig(cfg.GeneralConfig)))
	go deleteOperators(ctx, httpCli)
	streams := make(map[uint64]pdpb.PD_RegionHeartbeatClient, cfg.StoreCount)
	for i := 1; i <= cfg.StoreCount; i++ {
		clis[uint64(i)], streams[uint64(i)] = createHeartbeatStream(ctx, cfg)
	}
	header := &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
	var heartbeatTicker = time.NewTicker(regionReportInterval * time.Second)
	defer heartbeatTicker.Stop()
	var resolvedTSTicker = time.NewTicker(time.Second)
	defer resolvedTSTicker.Stop()
	for {
		select {
		case <-heartbeatTicker.C:
			if cfg.Round != 0 && regions.updateRound > cfg.Round {
				os.Exit(0)
			}
			rep := newReport(cfg)
			r := rep.Stats()

			startTime := time.Now()
			wg := &sync.WaitGroup{}
			for i := 1; i <= cfg.StoreCount; i++ {
				id := uint64(i)
				wg.Add(1)
				go regions.handleRegionHeartbeat(wg, streams[id], id, rep)
			}
			wg.Wait()

			since := time.Since(startTime).Seconds()
			close(rep.Results())
			regions.result(cfg.RegionCount, since)
			stats := <-r
			log.Info("region heartbeat stats", zap.String("total", fmt.Sprintf("%.4fs", stats.Total.Seconds())),
				zap.String("slowest", fmt.Sprintf("%.4fs", stats.Slowest)),
				zap.String("fastest", fmt.Sprintf("%.4fs", stats.Fastest)),
				zap.String("average", fmt.Sprintf("%.4fs", stats.Average)),
				zap.String("stddev", fmt.Sprintf("%.4fs", stats.Stddev)),
				zap.String("rps", fmt.Sprintf("%.4f", stats.RPS)),
				zap.Uint64("max-epoch-version", maxVersion),
			)
			log.Info("store heartbeat stats", zap.String("max", fmt.Sprintf("%.4fs", since)))
			regions.update(cfg, options)
			go stores.update(regions) // update stores in background, unusually region heartbeat is slower than store update.
		case <-resolvedTSTicker.C:
			wg := &sync.WaitGroup{}
			for i := 1; i <= cfg.StoreCount; i++ {
				id := uint64(i)
				wg.Add(1)
				go func(wg *sync.WaitGroup, id uint64) {
					defer wg.Done()
					cli := clis[id]
					_, err := cli.ReportMinResolvedTS(ctx, &pdpb.ReportMinResolvedTsRequest{
						Header:        header,
						StoreId:       id,
						MinResolvedTs: uint64(time.Now().Unix()),
					})
					if err != nil {
						log.Error("send resolved TS error", zap.Uint64("store-id", id), zap.Error(err))
						return
					}
				}(wg, id)
			}
			wg.Wait()
		case <-ctx.Done():
			if err := srv.Shutdown(context.Background()); err != nil {
				log.Fatal("server shutdown error", zap.Error(err))
			}
			log.Info("got signal to exit")
			return
		}
	}
}

func runHTTPServer(cfg *config, options *options) *http.Server {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(cors.Default())
	engine.Use(gzip.Gzip(gzip.DefaultCompression))
	engine.GET("metrics", utils.PromHandler())
	// profile API
	pprof.Register(engine)
	engine.PUT("config", func(c *gin.Context) {
		newCfg := cfg.Clone()
		newCfg.HotStoreCount = options.GetHotStoreCount()
		newCfg.FlowUpdateRatio = options.GetFlowUpdateRatio()
		newCfg.LeaderUpdateRatio = options.GetLeaderUpdateRatio()
		newCfg.EpochUpdateRatio = options.GetEpochUpdateRatio()
		newCfg.SpaceUpdateRatio = options.GetSpaceUpdateRatio()
		newCfg.ReportRatio = options.GetReportRatio()
		if err := c.BindJSON(&newCfg); err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		if err := newCfg.Validate(); err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		options.SetOptions(newCfg)
		c.String(http.StatusOK, "Successfully updated the configuration")
	})
	engine.GET("config", func(c *gin.Context) {
		output := cfg.Clone()
		output.HotStoreCount = options.GetHotStoreCount()
		output.FlowUpdateRatio = options.GetFlowUpdateRatio()
		output.LeaderUpdateRatio = options.GetLeaderUpdateRatio()
		output.EpochUpdateRatio = options.GetEpochUpdateRatio()
		output.SpaceUpdateRatio = options.GetSpaceUpdateRatio()
		output.ReportRatio = options.GetReportRatio()

		c.IndentedJSON(http.StatusOK, output)
	})
	srv := &http.Server{Addr: cfg.StatusAddr, Handler: engine.Handler(), ReadHeaderTimeout: 3 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server listen error", zap.Error(err))
		}
	}()

	return srv
}
