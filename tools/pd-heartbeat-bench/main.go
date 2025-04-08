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

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/docker/go-units"
	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/spf13/pflag"
	"go.etcd.io/etcd/pkg/v3/report"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	pdHttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
	"github.com/tikv/pd/client/pkg/utils/tlsutil"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/tools/pd-heartbeat-bench/config"
	"github.com/tikv/pd/tools/pd-heartbeat-bench/metrics"
	"github.com/tikv/pd/tools/utils"
)

const (
	regionReportInterval = 60 // 60s
	storeReportInterval  = 10 // 10s
	capacity             = 4 * units.TiB
)

var (
	clusterID  uint64
	maxVersion uint64 = 1
)

func newClient(ctx context.Context, cfg *config.Config) (pdpb.PDClient, error) {
	tlsConfig, err := cfg.Security.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	cc, err := grpcutil.GetClientConn(ctx, cfg.PDAddr, tlsConfig)
	if err != nil {
		return nil, err
	}
	return pdpb.NewPDClient(cc), nil
}

func initClusterID(ctx context.Context, cli pdpb.PDClient) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cctx, cancel := context.WithCancel(ctx)
			res, err := cli.GetMembers(cctx, &pdpb.GetMembersRequest{})
			cancel()
			if err != nil {
				continue
			}
			if res.GetHeader().GetError() != nil {
				continue
			}
			clusterID = res.GetHeader().GetClusterId()
			log.Info("init cluster ID successfully", zap.Uint64("cluster-id", clusterID))
			return
		}
	}
}

func header() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
}

func bootstrap(ctx context.Context, cli pdpb.PDClient) {
	cctx, cancel := context.WithCancel(ctx)
	isBootstrapped, err := cli.IsBootstrapped(cctx, &pdpb.IsBootstrappedRequest{Header: header()})
	cancel()
	if err != nil {
		log.Fatal("check if cluster has already bootstrapped failed", zap.Error(err))
	}
	if isBootstrapped.GetBootstrapped() {
		log.Info("already bootstrapped")
		return
	}

	store := &metapb.Store{
		Id:      1,
		Address: fmt.Sprintf("localhost:%d", 2),
		Version: "6.4.0-alpha",
	}
	region := &metapb.Region{
		Id:          1,
		Peers:       []*metapb.Peer{{StoreId: 1, Id: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	req := &pdpb.BootstrapRequest{
		Header: header(),
		Store:  store,
		Region: region,
	}
	cctx, cancel = context.WithCancel(ctx)
	resp, err := cli.Bootstrap(cctx, req)
	cancel()
	if err != nil {
		log.Fatal("failed to bootstrap the cluster", zap.Error(err))
	}
	if resp.GetHeader().GetError() != nil {
		log.Fatal("failed to bootstrap the cluster", zap.String("err", resp.GetHeader().GetError().String()))
	}
	log.Info("bootstrapped")
}

func putStores(ctx context.Context, cfg *config.Config, cli pdpb.PDClient, stores *Stores) {
	for i := uint64(1); i <= uint64(cfg.StoreCount); i++ {
		store := &metapb.Store{
			Id:      i,
			Address: fmt.Sprintf("localhost:%d", i),
			Version: "6.4.0-alpha",
		}
		cctx, cancel := context.WithCancel(ctx)
		resp, err := cli.PutStore(cctx, &pdpb.PutStoreRequest{Header: header(), Store: store})
		cancel()
		if err != nil {
			log.Fatal("failed to put store", zap.Uint64("store-id", i), zap.Error(err))
		}
		if resp.GetHeader().GetError() != nil {
			log.Fatal("failed to put store", zap.Uint64("store-id", i), zap.String("err", resp.GetHeader().GetError().String()))
		}
		go func(ctx context.Context, storeID uint64) {
			heartbeatTicker := time.NewTicker(10 * time.Second)
			defer heartbeatTicker.Stop()
			for {
				select {
				case <-heartbeatTicker.C:
					stores.heartbeat(ctx, cli, storeID)
				case <-ctx.Done():
					return
				}
			}
		}(ctx, i)
	}
}

func createHeartbeatStream(ctx context.Context, cfg *config.Config) (pdpb.PDClient, pdpb.PD_RegionHeartbeatClient) {
	cli, err := newClient(ctx, cfg)
	if err != nil {
		log.Fatal("create client error", zap.Error(err))
	}
	stream, err := cli.RegionHeartbeat(ctx)
	if err != nil {
		log.Fatal("create stream error", zap.Error(err))
	}

	go func() {
		// do nothing
		for {
			stream.Recv()
		}
	}()
	return cli, stream
}

// Stores contains store stats with lock.
type Stores struct {
	stat []atomic.Value
}

func newStores(storeCount int) *Stores {
	return &Stores{
		stat: make([]atomic.Value, storeCount+1),
	}
}

func (s *Stores) heartbeat(ctx context.Context, cli pdpb.PDClient, storeID uint64) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cli.StoreHeartbeat(cctx, &pdpb.StoreHeartbeatRequest{Header: header(), Stats: s.stat[storeID].Load().(*pdpb.StoreStats)})
}

func (s *Stores) update(rs *utils.Regions) {
	stats := make([]*pdpb.StoreStats, len(s.stat))
	now := uint64(time.Now().Unix())
	for i := range stats {
		stats[i] = &pdpb.StoreStats{
			StoreId:    uint64(i),
			Capacity:   capacity,
			Available:  capacity,
			QueryStats: &pdpb.QueryStats{},
			PeerStats:  make([]*pdpb.PeerStat, 0),
			Interval: &pdpb.TimeInterval{
				StartTimestamp: now - storeReportInterval,
				EndTimestamp:   now,
			},
		}
	}
	var toUpdate []*pdpb.RegionHeartbeatRequest
	updatedRegions := rs.AwakenRegions.Load()
	if updatedRegions == nil {
		toUpdate = rs.Regions
	} else {
		toUpdate = updatedRegions.([]*pdpb.RegionHeartbeatRequest)
	}
	for _, region := range toUpdate {
		for _, peer := range region.Region.Peers {
			store := stats[peer.StoreId]
			store.UsedSize += region.ApproximateSize
			store.Available -= region.ApproximateSize
			store.RegionCount += 1
		}
		store := stats[region.Leader.StoreId]
		if region.BytesWritten != 0 {
			store.BytesWritten += region.BytesWritten
			store.BytesRead += region.BytesRead
			store.KeysWritten += region.KeysWritten
			store.KeysRead += region.KeysRead
			store.QueryStats.Get += region.QueryStats.Get
			store.QueryStats.Put += region.QueryStats.Put
			store.PeerStats = append(store.PeerStats, &pdpb.PeerStat{
				RegionId:     region.Region.Id,
				ReadKeys:     region.KeysRead,
				ReadBytes:    region.BytesRead,
				WrittenKeys:  region.KeysWritten,
				WrittenBytes: region.BytesWritten,
				QueryStats:   region.QueryStats,
			})
		}
	}
	for i := range stats {
		s.stat[i].Store(stats[i])
	}
}

func main() {
	rand.New(rand.NewSource(0)) // Ensure consistent behavior multiple times
	statistics.Denoising = false
	cfg := config.NewConfig()
	err := cfg.Parse(os.Args[1:])
	defer logutil.LogPanic()

	switch errors.Cause(err) {
	case nil:
	case pflag.ErrHelp:
		exit(0)
	default:
		log.Fatal("parse cmd flags error", zap.Error(err))
	}

	// New zap logger
	err = logutil.SetupLogger(&cfg.Log, &cfg.Logger, &cfg.LogProps, logutil.RedactInfoLogOFF)
	if err == nil {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	} else {
		log.Fatal("initialize logger error", zap.Error(err))
	}

	maxVersion = cfg.InitEpochVer
	options := config.NewOptions(cfg)
	// let PD have enough time to start
	time.Sleep(5 * time.Second)
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
	cli, err := newClient(ctx, cfg)
	if err != nil {
		log.Fatal("create client error", zap.Error(err))
	}

	initClusterID(ctx, cli)
	go runHTTPServer(cfg, options)
	regions := utils.NewRegions(cfg.RegionCount, cfg.Replica, header())
	log.Info("finish init regions")
	stores := newStores(cfg.StoreCount)
	stores.update(regions)
	bootstrap(ctx, cli)
	putStores(ctx, cfg, cli, stores)
	log.Info("finish put stores")
	clis := make(map[uint64]pdpb.PDClient, cfg.StoreCount)
	httpCli := pdHttp.NewClient("tools-heartbeat-bench", []string{cfg.PDAddr}, pdHttp.WithTLSConfig(loadTLSConfig(cfg)))
	go deleteOperators(ctx, httpCli)
	streams := make(map[uint64]pdpb.PD_RegionHeartbeatClient, cfg.StoreCount)
	for i := 1; i <= cfg.StoreCount; i++ {
		clis[uint64(i)], streams[uint64(i)] = createHeartbeatStream(ctx, cfg)
	}
	header := &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
	heartbeatTicker := time.NewTicker(regionReportInterval * time.Second)
	defer heartbeatTicker.Stop()
	resolvedTSTicker := time.NewTicker(time.Second)
	defer resolvedTSTicker.Stop()
	withMetric := metrics.InitMetric2Collect(cfg.MetricsAddr)
	for {
		select {
		case <-heartbeatTicker.C:
			if cfg.Round != 0 && regions.UpdateRound > cfg.Round {
				exit(0)
			}
			rep := newReport(cfg)
			r := rep.Stats()

			startTime := time.Now()
			wg := &sync.WaitGroup{}
			for i := 1; i <= cfg.StoreCount; i++ {
				id := uint64(i)
				wg.Add(1)
				go regions.HandleRegionHeartbeat(wg, streams[id], id, rep)
			}
			if withMetric {
				metrics.CollectMetrics(regions.UpdateRound, time.Second)
			}
			wg.Wait()

			since := time.Since(startTime).Seconds()
			close(rep.Results())
			regions.Result(cfg.RegionCount, since)
			stats := <-r
			log.Info("region heartbeat stats",
				metrics.RegionFields(stats, zap.Uint64("max-epoch-version", maxVersion))...)
			log.Info("store heartbeat stats", zap.String("max", fmt.Sprintf("%.4fs", since)))
			metrics.CollectRegionAndStoreStats(&stats, &since)
			regions.Update(cfg.RegionCount, cfg.Replica, options)
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
			log.Info("got signal to exit")
			switch sig {
			case syscall.SIGTERM:
				exit(0)
			default:
				exit(1)
			}
		}
	}
}

func exit(code int) {
	metrics.OutputConclusion()
	os.Exit(code)
}

func deleteOperators(ctx context.Context, httpCli pdHttp.Client) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := httpCli.DeleteOperators(ctx)
			if err != nil {
				log.Error("fail to delete operators", zap.Error(err))
			}
		}
	}
}

func newReport(cfg *config.Config) report.Report {
	p := "%4.4f"
	if cfg.Sample {
		return report.NewReportSample(p)
	}
	return report.NewReport(p)
}

func runHTTPServer(cfg *config.Config, options *config.Options) {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(cors.Default())
	engine.Use(gzip.Gzip(gzip.DefaultCompression))
	engine.GET("metrics", mcsutils.PromHandler())
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
	engine.GET("metrics-collect", func(c *gin.Context) {
		second := c.Query("second")
		if second == "" {
			c.String(http.StatusBadRequest, "missing second")
			return
		}
		secondInt, err := strconv.Atoi(second)
		if err != nil {
			c.String(http.StatusBadRequest, "invalid second")
			return
		}
		metrics.CollectMetrics(metrics.WarmUpRound, time.Duration(secondInt)*time.Second)
		c.IndentedJSON(http.StatusOK, "Successfully collect metrics")
	})

	engine.Run(cfg.StatusAddr)
}

func loadTLSConfig(cfg *config.Config) *tls.Config {
	if len(cfg.Security.CAPath) == 0 {
		return nil
	}
	caData, err := os.ReadFile(cfg.Security.CAPath)
	if err != nil {
		log.Error("fail to read ca file", zap.Error(err))
	}
	certData, err := os.ReadFile(cfg.Security.CertPath)
	if err != nil {
		log.Error("fail to read cert file", zap.Error(err))
	}
	keyData, err := os.ReadFile(cfg.Security.KeyPath)
	if err != nil {
		log.Error("fail to read key file", zap.Error(err))
	}

	tlsConf, err := tlsutil.TLSConfig{
		SSLCABytes:   caData,
		SSLCertBytes: certData,
		SSLKEYBytes:  keyData,
	}.ToTLSConfig()
	if err != nil {
		log.Fatal("failed to load tlc config", zap.Error(err))
	}

	return tlsConf
}
