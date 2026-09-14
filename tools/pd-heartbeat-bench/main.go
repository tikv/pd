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
	"container/heap"
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

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
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/tools/pd-heartbeat-bench/config"
	"github.com/tikv/pd/tools/pd-heartbeat-bench/metrics"
	"github.com/tikv/pd/tools/utils"
)

const (
	regionReportInterval              = 60 // 60s
	storeReportInterval               = 10 // 10s
	storeHeartbeatsPerRegionHeartbeat = regionReportInterval / storeReportInterval
	// Match TiKV's hot-peer thresholds and per-dimension report capacity.
	hotPeerByteReportThreshold  uint64 = 8 * 1024 * storeReportInterval
	hotPeerKeyReportThreshold   uint64 = 128 * storeReportInterval
	hotPeerQueryReportThreshold uint64 = 128 * storeReportInterval
	hotPeerReportCapacity              = 1000
	hotPeerReportMetricCount           = 3
	storeRequestTimeout                = 5 * time.Second
)

var clusterID uint64

func newClient(ctx context.Context, cfg *config.Config) (pdpb.PDClient, error) {
	tlsConfig, err := cfg.Security.ToClientTLSConfig()
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
		Address: "mock://tikv-1:1",
		Version: "9.0.0-alpha.1",
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

func putStores(ctx context.Context, cfg *config.Config, cli pdpb.PDClient) {
	for i := uint64(1); i <= uint64(cfg.StoreCount); i++ {
		store := &metapb.Store{
			Id:      i,
			Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
			Version: "9.0.0-alpha.1",
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
	}
}

func createHeartbeatStream(
	ctx context.Context,
	cfg *config.Config,
	storeID uint64,
	streamErrCh chan<- error,
) (pdpb.PDClient, pdpb.PD_RegionHeartbeatClient) {
	cli, err := newClient(ctx, cfg)
	if err != nil {
		log.Fatal("create client error", zap.Error(err))
	}
	stream, err := cli.RegionHeartbeat(ctx)
	if err != nil {
		log.Fatal("create stream error", zap.Error(err))
	}

	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				select {
				case streamErrCh <- errors.Annotatef(err, "store %d region heartbeat stream failed", storeID):
				case <-ctx.Done():
				}
				return
			}
			if resp.GetHeader().GetError() != nil {
				log.Error("region heartbeat response error",
					zap.Uint64("store-id", storeID),
					zap.String("error", resp.GetHeader().GetError().String()))
			}
		}
	}()
	return cli, stream
}

// Stores contains store stats with lock.
type Stores struct {
	stat                      []atomic.Value
	lastHeartbeat             []atomic.Uint64
	capacity                  uint64
	heartbeatFailureMu        sync.Mutex
	failedStoreHeartbeatCount uint64
	lastFailedStoreID         uint64
	lastStoreHeartbeatError   string
}

func newStores(storeCount int, capacity uint64) *Stores {
	stores := &Stores{
		stat:          make([]atomic.Value, storeCount+1),
		lastHeartbeat: make([]atomic.Uint64, storeCount+1),
		capacity:      capacity,
	}
	now := uint64(time.Now().Unix())
	for storeID := 1; storeID <= storeCount; storeID++ {
		stores.lastHeartbeat[storeID].Store(now)
	}
	return stores
}

func (s *Stores) heartbeat(ctx context.Context, cli pdpb.PDClient, storeID uint64) {
	template := s.stat[storeID].Load()
	if template == nil {
		log.Error("store heartbeat stats are not initialized", zap.Uint64("store-id", storeID))
		return
	}
	stats := *template.(*pdpb.StoreStats)
	now := uint64(time.Now().Unix())
	stats.Interval = &pdpb.TimeInterval{
		StartTimestamp: s.lastHeartbeat[storeID].Swap(now),
		EndTimestamp:   now,
	}
	cctx, cancel := context.WithTimeout(ctx, storeRequestTimeout)
	defer cancel()
	resp, err := cli.StoreHeartbeat(cctx, &pdpb.StoreHeartbeatRequest{Header: header(), Stats: &stats})
	if err == nil && resp.GetHeader().GetError() != nil {
		err = errors.New(resp.GetHeader().GetError().String())
	}
	if err != nil {
		s.heartbeatFailureMu.Lock()
		s.failedStoreHeartbeatCount++
		s.lastFailedStoreID = storeID
		s.lastStoreHeartbeatError = err.Error()
		s.heartbeatFailureMu.Unlock()
	}
}

func (s *Stores) takeStoreHeartbeatFailures() (count, storeID uint64, err string) {
	s.heartbeatFailureMu.Lock()
	defer s.heartbeatFailureMu.Unlock()
	count = s.failedStoreHeartbeatCount
	s.failedStoreHeartbeatCount = 0
	return count, s.lastFailedStoreID, s.lastStoreHeartbeatError
}

func (s *Stores) reportStoreHeartbeatFailures(ctx context.Context, heartbeatWorkers *sync.WaitGroup) {
	reportTicker := time.NewTicker(time.Duration(storeReportInterval) * time.Second)
	defer reportTicker.Stop()
	report := func() {
		count, storeID, err := s.takeStoreHeartbeatFailures()
		if count == 0 {
			return
		}
		log.Error("store heartbeats failed",
			zap.Uint64("count", count),
			zap.Uint64("last-store-id", storeID),
			zap.String("last-error", err))
	}
	for {
		select {
		case <-reportTicker.C:
			report()
		case <-ctx.Done():
			heartbeatWorkers.Wait()
			report()
			return
		}
	}
}

func (s *Stores) update(rs *utils.Regions) {
	stats := make([]*pdpb.StoreStats, len(s.stat))
	for i := 1; i < len(stats); i++ {
		stats[i] = &pdpb.StoreStats{
			StoreId:    uint64(i),
			Capacity:   s.capacity,
			Available:  s.capacity,
			QueryStats: &pdpb.QueryStats{},
			PeerStats:  make([]*pdpb.PeerStat, 0),
		}
	}
	// Silent Regions still occupy space and count toward the store's complete
	// Region inventory.
	for _, region := range rs.Regions {
		for _, peer := range region.Region.Peers {
			store := stats[peer.StoreId]
			store.UsedSize += region.ApproximateSize
			store.RegionCount += 1
		}
	}
	for i := 1; i < len(stats); i++ {
		stats[i].Available = s.capacity - min(s.capacity, stats[i].UsedSize)
	}

	// Region heartbeat flow values cover a 60-second interval. StoreHeartbeat
	// reports every 10 seconds, so report one sixth on each tick. Only awake
	// Regions contribute flow; a silent Region has no new activity by definition.
	for _, region := range rs.ReportedRegions() {
		store := stats[region.Leader.StoreId]
		if hasRegionFlow(region) {
			store.BytesWritten += region.BytesWritten / storeHeartbeatsPerRegionHeartbeat
			store.BytesRead += region.BytesRead / storeHeartbeatsPerRegionHeartbeat
			store.KeysWritten += region.KeysWritten / storeHeartbeatsPerRegionHeartbeat
			store.KeysRead += region.KeysRead / storeHeartbeatsPerRegionHeartbeat
			store.QueryStats.Get += region.QueryStats.Get / storeHeartbeatsPerRegionHeartbeat
			store.QueryStats.Put += region.QueryStats.Put / storeHeartbeatsPerRegionHeartbeat
			peerStat := &pdpb.PeerStat{
				RegionId:     region.Region.Id,
				ReadKeys:     region.KeysRead / storeHeartbeatsPerRegionHeartbeat,
				ReadBytes:    region.BytesRead / storeHeartbeatsPerRegionHeartbeat,
				WrittenKeys:  region.KeysWritten / storeHeartbeatsPerRegionHeartbeat,
				WrittenBytes: region.BytesWritten / storeHeartbeatsPerRegionHeartbeat,
				QueryStats: &pdpb.QueryStats{
					Get: region.QueryStats.Get / storeHeartbeatsPerRegionHeartbeat,
					Put: region.QueryStats.Put / storeHeartbeatsPerRegionHeartbeat,
				},
			}
			if shouldReportReadPeerStat(peerStat) {
				store.PeerStats = append(store.PeerStats, peerStat)
			}
		}
	}
	for i := 1; i < len(stats); i++ {
		stats[i].PeerStats = selectHotPeerStats(stats[i].PeerStats)
		s.stat[i].Store(stats[i])
	}
}

func shouldReportReadPeerStat(peerStat *pdpb.PeerStat) bool {
	return peerStat.GetReadBytes() >= hotPeerByteReportThreshold ||
		peerStat.GetReadKeys() >= hotPeerKeyReportThreshold ||
		readPeerQueryCount(peerStat) >= hotPeerQueryReportThreshold
}

func readPeerQueryCount(peerStat *pdpb.PeerStat) uint64 {
	queryStats := peerStat.GetQueryStats()
	return queryStats.GetGet() + queryStats.GetCoprocessor() + queryStats.GetScan()
}

type rankedPeerStat struct {
	peerStat *pdpb.PeerStat
	value    uint64
}

type rankedPeerStatHeap []rankedPeerStat

func (h rankedPeerStatHeap) Len() int { return len(h) }

func (h rankedPeerStatHeap) Less(i, j int) bool {
	if h[i].value != h[j].value {
		return h[i].value < h[j].value
	}
	return h[i].peerStat.GetRegionId() > h[j].peerStat.GetRegionId()
}

func (h rankedPeerStatHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *rankedPeerStatHeap) Push(value any) {
	*h = append(*h, value.(rankedPeerStat))
}

func (h *rankedPeerStatHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

func isBetterRankedPeerStat(candidate, current rankedPeerStat) bool {
	return candidate.value > current.value ||
		(candidate.value == current.value && candidate.peerStat.GetRegionId() < current.peerStat.GetRegionId())
}

func addTopPeerStats(
	selected map[uint64]struct{},
	peerStats []*pdpb.PeerStat,
	value func(*pdpb.PeerStat) uint64,
) {
	top := make(rankedPeerStatHeap, 0, hotPeerReportCapacity)
	heap.Init(&top)
	for _, peerStat := range peerStats {
		candidate := rankedPeerStat{peerStat: peerStat, value: value(peerStat)}
		if top.Len() < hotPeerReportCapacity {
			heap.Push(&top, candidate)
			continue
		}
		if isBetterRankedPeerStat(candidate, top[0]) {
			heap.Pop(&top)
			heap.Push(&top, candidate)
		}
	}
	for _, ranked := range top {
		selected[ranked.peerStat.GetRegionId()] = struct{}{}
	}
}

func selectHotPeerStats(peerStats []*pdpb.PeerStat) []*pdpb.PeerStat {
	// TiKV reports the union of a bounded Top-N set for each generated read
	// dimension. Per-Region CPU is not simulated by this benchmark.
	if len(peerStats) < hotPeerReportCapacity*hotPeerReportMetricCount {
		return peerStats
	}

	selected := make(map[uint64]struct{}, hotPeerReportCapacity*hotPeerReportMetricCount)
	addTopPeerStats(selected, peerStats, func(peerStat *pdpb.PeerStat) uint64 {
		return peerStat.GetReadKeys()
	})
	addTopPeerStats(selected, peerStats, func(peerStat *pdpb.PeerStat) uint64 {
		return peerStat.GetReadBytes()
	})
	addTopPeerStats(selected, peerStats, func(peerStat *pdpb.PeerStat) uint64 {
		return readPeerQueryCount(peerStat)
	})

	result := make([]*pdpb.PeerStat, 0, len(selected))
	for _, peerStat := range peerStats {
		if _, ok := selected[peerStat.GetRegionId()]; ok {
			result = append(result, peerStat)
		}
	}
	return result
}

func hasRegionFlow(region *pdpb.RegionHeartbeatRequest) bool {
	return region.GetBytesWritten() != 0 || region.GetBytesRead() != 0 ||
		region.GetKeysWritten() != 0 || region.GetKeysRead() != 0 ||
		region.GetQueryStats().GetGet() != 0 || region.GetQueryStats().GetPut() != 0
}

func startStoreHeartbeats(ctx context.Context, clis map[uint64]pdpb.PDClient, stores *Stores) <-chan struct{} {
	heartbeatWorkers := &sync.WaitGroup{}
	for storeID, cli := range clis {
		heartbeatWorkers.Add(1)
		go func(storeID uint64, cli pdpb.PDClient) {
			defer heartbeatWorkers.Done()
			ticker := time.NewTicker(storeReportInterval * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					stores.heartbeat(ctx, cli, storeID)
				case <-ctx.Done():
					return
				}
			}
		}(storeID, cli)
	}
	reporterDone := make(chan struct{})
	go func() {
		defer close(reporterDone)
		stores.reportStoreHeartbeatFailures(ctx, heartbeatWorkers)
	}()
	return reporterDone
}

func startMinResolvedTSReports(ctx context.Context, clis map[uint64]pdpb.PDClient) {
	requestHeader := &pdpb.RequestHeader{ClusterId: clusterID}
	for storeID, cli := range clis {
		go func(storeID uint64, cli pdpb.PDClient) {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					cctx, cancel := context.WithTimeout(ctx, storeRequestTimeout)
					resp, err := cli.ReportMinResolvedTS(cctx, &pdpb.ReportMinResolvedTsRequest{
						Header:        requestHeader,
						StoreId:       storeID,
						MinResolvedTs: tsoutil.TimeToTS(time.Now()),
					})
					cancel()
					if err != nil {
						log.Error("failed to report minimum resolved TS", zap.Uint64("store-id", storeID), zap.Error(err))
						continue
					}
					if resp.GetHeader().GetError() != nil {
						log.Error("minimum resolved TS response error",
							zap.Uint64("store-id", storeID),
							zap.String("error", resp.GetHeader().GetError().String()))
					}
				case <-ctx.Done():
					return
				}
			}
		}(storeID, cli)
	}
}

func main() {
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
	regions := utils.NewRegions(
		cfg.RegionCount,
		cfg.Replica,
		cfg.StoreCount,
		header(),
		utils.WithInitialVersion(cfg.InitEpochVer),
		utils.WithRegionSize(uint64(cfg.RegionSize)),
		utils.WithRegionKeys(cfg.RegionKeys),
		utils.WithRandomSeed(cfg.RandomSeed),
	)
	log.Info("finish init regions")
	stores := newStores(cfg.StoreCount, uint64(cfg.StoreCapacity))
	stores.update(regions)
	bootstrap(ctx, cli)
	putStores(ctx, cfg, cli)
	log.Info("finish put stores")
	clis := make(map[uint64]pdpb.PDClient, cfg.StoreCount)
	if cfg.DeleteOperators {
		log.Warn("periodic operator deletion is enabled; this creates a synthetic scheduler workload")
		httpCli := pdHttp.NewClient("tools-heartbeat-bench", []string{cfg.PDAddr}, pdHttp.WithTLSConfig(loadTLSConfig(cfg)))
		go deleteOperators(ctx, httpCli)
	}
	streams := make(map[uint64]pdpb.PD_RegionHeartbeatClient, cfg.StoreCount)
	streamErrCh := make(chan error, cfg.StoreCount)
	for i := 1; i <= cfg.StoreCount; i++ {
		storeID := uint64(i)
		clis[storeID], streams[storeID] = createHeartbeatStream(ctx, cfg, storeID, streamErrCh)
	}
	heartbeatReporterDone := startStoreHeartbeats(ctx, clis, stores)
	startMinResolvedTSReports(ctx, clis)
	heartbeatTimer := time.NewTimer(0)
	defer heartbeatTimer.Stop()
	withMetric := metrics.InitMetric2Collect(cfg.MetricsAddr)
	for {
		select {
		case <-heartbeatTimer.C:
			if cfg.Round != 0 && regions.UpdateRound > cfg.Round {
				cancel()
				<-heartbeatReporterDone
				exit(0)
			}
			rep := newReport(cfg)
			r := rep.Stats()

			startTime := time.Now()
			regions.PrepareReportIntervals(startTime)
			wg := &sync.WaitGroup{}
			regionsByLeader := regions.GroupReportedRegionsByLeader(cfg.StoreCount)
			for i := 1; i <= cfg.StoreCount; i++ {
				id := uint64(i)
				wg.Add(1)
				go regions.HandleRegionHeartbeat(ctx, wg, streams[id], id, regionsByLeader[id], rep)
			}
			metricDone := make(chan struct{})
			if withMetric {
				go func() {
					metrics.CollectMetrics(regions.UpdateRound, time.Second)
					close(metricDone)
				}()
			} else {
				close(metricDone)
			}
			wg.Wait()

			since := time.Since(startTime).Seconds()
			close(rep.Results())
			regions.Result(since)
			stats := <-r
			log.Info("region heartbeat client send stats",
				metrics.RegionFields(stats, zap.Uint64("max-epoch-version", regions.MaxVersion()))...)
			log.Info("region heartbeat round stats", zap.String("duration", fmt.Sprintf("%.4fs", since)))
			if regions.UpdateRound >= metrics.WarmUpRound {
				metrics.CollectRegionAndStoreStats(&stats, &since)
			}
			regions.Update(options)
			stores.update(regions)
			<-metricDone
			if cfg.Round != 0 && regions.UpdateRound > cfg.Round {
				cancel()
				<-heartbeatReporterDone
				exit(0)
			}
			delay := regionReportInterval*time.Second - time.Since(startTime)
			heartbeatTimer.Reset(max(delay, 0))
		case err := <-streamErrCh:
			log.Error("region heartbeat stream stopped", zap.Error(err))
			cancel()
			<-heartbeatReporterDone
			exit(1)
		case <-ctx.Done():
			log.Info("got signal to exit")
			<-heartbeatReporterDone
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
		applyWorkloadOptions(newCfg, options.Snapshot())
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
		applyWorkloadOptions(output, options.Snapshot())

		c.IndentedJSON(http.StatusOK, output)
	})
	engine.GET("metrics-collect", func(c *gin.Context) {
		second := c.Query("second")
		if second == "" {
			c.String(http.StatusBadRequest, "missing second")
			return
		}
		secondInt, err := strconv.Atoi(second)
		if err != nil || secondInt <= 0 {
			c.String(http.StatusBadRequest, "invalid second")
			return
		}
		metrics.CollectMetrics(metrics.WarmUpRound, time.Duration(secondInt)*time.Second)
		c.IndentedJSON(http.StatusOK, "Successfully collect metrics")
	})

	if err := engine.Run(cfg.StatusAddr); err != nil && !errors.ErrorEqual(err, http.ErrServerClosed) {
		log.Error("heartbeat bench HTTP server stopped", zap.Error(err))
	}
}

func applyWorkloadOptions(cfg *config.Config, options config.WorkloadOptions) {
	cfg.HotStoreCount = options.HotStoreCount
	cfg.ReportRatio = options.ReportRatio
	cfg.LeaderUpdateRatio = options.LeaderUpdateRatio
	cfg.EpochUpdateRatio = options.EpochUpdateRatio
	cfg.SpaceUpdateRatio = options.SpaceUpdateRatio
	cfg.FlowUpdateRatio = options.FlowUpdateRatio
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
