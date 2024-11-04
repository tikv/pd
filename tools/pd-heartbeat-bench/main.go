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
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/tools/pd-heartbeat-bench/config"
	"go.etcd.io/etcd/pkg/report"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	bytesUnit    = 1 << 23 // 8MB
	keysUint     = 1 << 13 // 8K
	intervalUint = 60      // 60s
)

var clusterID uint64

func trimHTTPPrefix(str string) string {
	str = strings.TrimPrefix(str, "http://")
	str = strings.TrimPrefix(str, "https://")
	return str
}

func newClient(cfg *config.Config) pdpb.PDClient {
	addr := trimHTTPPrefix(cfg.PDAddr)
	cc, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal("failed to create gRPC connection", zap.Error(err))
	}
	return pdpb.NewPDClient(cc)
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
	log.Info("init cluster ID successfully", zap.Uint64("cluster-id", clusterID))
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

func putStores(ctx context.Context, cfg *config.Config, cli pdpb.PDClient) {
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
			var heartbeatTicker = time.NewTicker(10 * time.Second)
			defer heartbeatTicker.Stop()
			for {
				select {
				case <-heartbeatTicker.C:
					cctx, cancel := context.WithCancel(ctx)
					cli.StoreHeartbeat(cctx, &pdpb.StoreHeartbeatRequest{Header: header(), Stats: &pdpb.StoreStats{
						StoreId:   storeID,
						Capacity:  2 * units.TiB,
						Available: 1.5 * units.TiB,
					}})
					cancel()
				case <-ctx.Done():
					return
				}
			}
		}(ctx, i)
	}
}

func newStartKey(id uint64, keyLen int) []byte {
	k := make([]byte, keyLen)
	copy(k, fmt.Sprintf("%010d", id))
	return k
}

func newEndKey(id uint64, keyLen int) []byte {
	k := newStartKey(id, keyLen)
	k[len(k)-1]++
	return k
}

// Regions simulates all regions to heartbeat.
type Regions struct {
	regions []*pdpb.RegionHeartbeatRequest

	updateRound int

	updateLeader []int
	updateEpoch  []int
	updateSpace  []int
	updateFlow   []int
}

func (rs *Regions) init(cfg *config.Config) {
	rs.regions = make([]*pdpb.RegionHeartbeatRequest, 0, cfg.RegionCount)
	rs.updateRound = 0

	// Generate regions
	id := uint64(1)
	now := uint64(time.Now().Unix())

	keyLen := cfg.KeyLength
	for i := 0; i < cfg.RegionCount; i++ {
		region := &pdpb.RegionHeartbeatRequest{
			Header: header(),
			Region: &metapb.Region{
				Id:          id,
				StartKey:    newStartKey(id, keyLen),
				EndKey:      newEndKey(id, keyLen),
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 1},
			},
			ApproximateSize: bytesUnit,
			Interval: &pdpb.TimeInterval{
				StartTimestamp: now,
				EndTimestamp:   now + intervalUint,
			},
			ApproximateKeys: keysUint,
			Term:            1,
		}
		id += 1

		peers := make([]*metapb.Peer, 0, cfg.Replica)
		for j := 0; j < cfg.Replica; j++ {
			peers = append(peers, &metapb.Peer{Id: id, StoreId: uint64((i+j)%cfg.StoreCount + 1)})
			id += 1
		}

		region.Region.Peers = peers
		region.Leader = peers[0]
		rs.regions = append(rs.regions, region)
	}

	// Generate sample index
	slice := make([]int, cfg.RegionCount)
	for i := range slice {
		slice[i] = i
	}

	rand.New(rand.NewSource(0)) // Ensure consistent behavior multiple times
	pick := func(ratio float64) []int {
		rand.Shuffle(cfg.RegionCount, func(i, j int) {
			slice[i], slice[j] = slice[j], slice[i]
		})
		return append(slice[:0:0], slice[0:int(float64(cfg.RegionCount)*ratio)]...)
	}

	rs.updateLeader = pick(cfg.LeaderUpdateRatio)
	rs.updateEpoch = pick(cfg.EpochUpdateRatio)
	rs.updateSpace = pick(cfg.SpaceUpdateRatio)
	rs.updateFlow = pick(cfg.FlowUpdateRatio)
}

func (rs *Regions) update(replica int) {
	rs.updateRound += 1

	// update leader
	for _, i := range rs.updateLeader {
		region := rs.regions[i]
		region.Leader = region.Region.Peers[rs.updateRound%replica]
	}
	// update epoch
	for _, i := range rs.updateEpoch {
		region := rs.regions[i]
		region.Region.RegionEpoch.Version += 1
	}
	// update space
	for _, i := range rs.updateSpace {
		region := rs.regions[i]
		region.ApproximateSize += bytesUnit
		region.ApproximateKeys += keysUint
	}
	// update flow
	for _, i := range rs.updateFlow {
		region := rs.regions[i]
		region.BytesWritten += bytesUnit
		region.BytesRead += bytesUnit
		region.KeysWritten += keysUint
		region.KeysRead += keysUint
	}
	// update interval
	for _, region := range rs.regions {
		region.Interval.StartTimestamp = region.Interval.EndTimestamp
		region.Interval.EndTimestamp = region.Interval.StartTimestamp + intervalUint
	}
}

func createHeartbeatStream(ctx context.Context, cfg *config.Config) pdpb.PD_RegionHeartbeatClient {
	cli := newClient(cfg)
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
	return stream
}

func (rs *Regions) handleRegionHeartbeat(wg *sync.WaitGroup, stream pdpb.PD_RegionHeartbeatClient, storeID uint64, rep report.Report) {
	defer wg.Done()
	var regions []*pdpb.RegionHeartbeatRequest
	for _, region := range rs.regions {
		if region.Leader.StoreId != storeID {
			continue
		}
		regions = append(regions, region)
	}

	start := time.Now()
	var err error
	for _, region := range regions {
		err = stream.Send(region)
		rep.Results() <- report.Result{Start: start, End: time.Now(), Err: err}
		if err == io.EOF {
			log.Error("receive eof error", zap.Uint64("store-id", storeID), zap.Error(err))
			err := stream.CloseSend()
			if err != nil {
				log.Error("fail to close stream", zap.Uint64("store-id", storeID), zap.Error(err))
			}
			return
		}
		if err != nil {
			log.Error("send result error", zap.Uint64("store-id", storeID), zap.Error(err))
			return
		}
	}
	log.Info("store finish one round region heartbeat", zap.Uint64("store-id", storeID), zap.Duration("cost-time", time.Since(start)))
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
	err = cfg.SetupLogger()
	if err == nil {
		log.ReplaceGlobals(cfg.GetZapLogger(), cfg.GetZapLogProperties())
	} else {
		log.Fatal("initialize logger error", zap.Error(err))
	}

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
	cli := newClient(cfg)
	initClusterID(ctx, cli)
	bootstrap(ctx, cli)
	putStores(ctx, cfg, cli)
	log.Info("finish put stores")
	regions := new(Regions)
	regions.init(cfg)
	log.Info("finish init regions")

	streams := make(map[uint64]pdpb.PD_RegionHeartbeatClient, cfg.StoreCount)
	for i := 1; i <= cfg.StoreCount; i++ {
		streams[uint64(i)] = createHeartbeatStream(ctx, cfg)
	}
	var heartbeatTicker = time.NewTicker(60 * time.Second)
	defer heartbeatTicker.Stop()
	for {
		select {
		case <-heartbeatTicker.C:
			if cfg.Round != 0 && regions.updateRound > cfg.Round {
				exit(0)
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
			)
			log.Info("store heartbeat stats", zap.String("max", fmt.Sprintf("%.4fs", since)))
			regions.update(cfg.Replica)
		case <-ctx.Done():
			log.Info("Got signal to exit")
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
	os.Exit(code)
}

func newReport(cfg *config.Config) report.Report {
	p := "%4.4f"
	if cfg.Sample {
		return report.NewReportSample(p)
	}
	return report.NewReport(p)
}

func (rs *Regions) result(regionCount int, sec float64) {
	if rs.updateRound == 0 {
		// There was no difference in the first round
		return
	}

	updated := make(map[int]struct{})
	for _, i := range rs.updateLeader {
		updated[i] = struct{}{}
	}
	for _, i := range rs.updateEpoch {
		updated[i] = struct{}{}
	}
	for _, i := range rs.updateSpace {
		updated[i] = struct{}{}
	}
	for _, i := range rs.updateFlow {
		updated[i] = struct{}{}
	}
	inactiveCount := regionCount - len(updated)

	log.Info("update speed of each category", zap.String("rps", fmt.Sprintf("%.4f", float64(regionCount)/sec)),
		zap.String("save-tree", fmt.Sprintf("%.4f", float64(len(rs.updateLeader))/sec)),
		zap.String("save-kv", fmt.Sprintf("%.4f", float64(len(rs.updateEpoch))/sec)),
		zap.String("save-space", fmt.Sprintf("%.4f", float64(len(rs.updateSpace))/sec)),
		zap.String("save-flow", fmt.Sprintf("%.4f", float64(len(rs.updateFlow))/sec)),
		zap.String("skip", fmt.Sprintf("%.4f", float64(inactiveCount)/sec)))
}
