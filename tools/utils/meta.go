// Copyright 2025 TiKV Project Authors.
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

package utils

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"go.etcd.io/etcd/pkg/v3/report"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/tools/pd-heartbeat-bench/config"
)

// BootstrapCluster tries to bootstrap a cluster with the given header and version.
func BootstrapCluster(ctx context.Context, cli pdpb.PDClient, header *pdpb.RequestHeader, version string) {
	cctx, cancel := context.WithCancel(ctx)
	isBootstrapped, err := cli.IsBootstrapped(cctx, &pdpb.IsBootstrappedRequest{Header: header})
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
		Version: version,
	}
	region := &metapb.Region{
		Id:          1,
		Peers:       []*metapb.Peer{{StoreId: 1, Id: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	req := &pdpb.BootstrapRequest{
		Header: header,
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
}

// PutStores puts the given stores to the cluster.
func PutStores(ctx context.Context, cli pdpb.PDClient, header *pdpb.RequestHeader, stores []*metapb.Store) {
	for _, store := range stores {
		storeID := store.GetId()
		cctx, cancel := context.WithCancel(ctx)
		resp, err := cli.PutStore(cctx, &pdpb.PutStoreRequest{Header: header, Store: store})
		cancel()
		if err != nil {
			log.Fatal("failed to put store", zap.Uint64("store-id", storeID), zap.Error(err))
		}
		if resp.GetHeader().GetError() != nil {
			log.Fatal("failed to put store", zap.Uint64("store-id", storeID), zap.String("err", resp.GetHeader().GetError().String()))
		}

		go func(ctx context.Context, storeID uint64) {
			heartbeatTicker := time.NewTicker(10 * time.Second)
			defer heartbeatTicker.Stop()
			for {
				select {
				case <-heartbeatTicker.C:
					cctx, cancel := context.WithCancel(ctx)
					_, err := cli.StoreHeartbeat(cctx, &pdpb.StoreHeartbeatRequest{
						Header: header,
						Stats: &pdpb.StoreStats{
							StoreId: storeID,
						},
					})
					cancel()
					if err != nil {
						log.Error("failed to send store heartbeat", zap.Uint64("store-id", storeID), zap.Error(err))
					}
				case <-ctx.Done():
					return
				}
			}
		}(ctx, storeID)
	}
}

const (
	defaultRegionSize    = 256 * units.MiB
	defaultRegionKeys    = 2560000
	coldByteUnit         = 128
	coldKeyUnit          = 8
	queryUnit            = 8
	hotByteUnit          = 16 * units.KiB
	hotKeysUint          = 256
	hotQueryUnit         = 256
	regionReportInterval = 60 // 60s
)

type regionOptions struct {
	initialVersion uint64
	regionSize     uint64
	regionKeys     uint64
	randomSeed     uint64
}

// RegionOption configures generated regions.
type RegionOption func(*regionOptions)

// WithInitialVersion sets the initial Region epoch version.
func WithInitialVersion(version uint64) RegionOption {
	return func(opts *regionOptions) {
		opts.initialVersion = version
	}
}

// WithRegionSize sets the initial approximate Region size in bytes.
func WithRegionSize(size uint64) RegionOption {
	return func(opts *regionOptions) {
		opts.regionSize = size
	}
}

// WithRegionKeys sets the initial approximate key count of each Region.
func WithRegionKeys(keys uint64) RegionOption {
	return func(opts *regionOptions) {
		opts.regionKeys = keys
	}
}

// WithRandomSeed sets the seed used to select and update Regions.
func WithRandomSeed(seed uint64) RegionOption {
	return func(opts *regionOptions) {
		opts.randomSeed = seed
	}
}

// Regions simulates all regions to heartbeat.
type Regions struct {
	regionCount         int
	replicaCount        int
	maxVersion          uint64
	regionSize          uint64
	regionKeys          uint64
	reportOrder         []int
	rng                 *rand.Rand
	lastReportTimestamp uint64
	// Regions is the list of all regions to heartbeat.
	Regions []*pdpb.RegionHeartbeatRequest
	// AwakenRegions contains the Regions that report in the current round.
	AwakenRegions atomic.Value

	UpdateRound int

	updateLeader []int
	updateEpoch  []int
	updateSpace  []int
	updateFlow   []int
}

// NewRegions initializes the regions with the given region count and replica count.
func NewRegions(regionCount, replicaCount, storeCount int, header *pdpb.RequestHeader, opts ...RegionOption) *Regions {
	options := regionOptions{
		initialVersion: 1,
		regionSize:     defaultRegionSize,
		regionKeys:     defaultRegionKeys,
		randomSeed:     1,
	}
	for _, opt := range opts {
		opt(&options)
	}
	rng := rand.New(rand.NewPCG(options.randomSeed, options.randomSeed^0x9e3779b97f4a7c15))
	now := uint64(time.Now().Unix())
	rs := &Regions{
		regionCount:         regionCount,
		replicaCount:        replicaCount,
		Regions:             make([]*pdpb.RegionHeartbeatRequest, 0, regionCount),
		UpdateRound:         0,
		maxVersion:          options.initialVersion,
		regionSize:          options.regionSize,
		regionKeys:          options.regionKeys,
		reportOrder:         make([]int, regionCount),
		rng:                 rng,
		lastReportTimestamp: now,
	}
	for i := range rs.reportOrder {
		rs.reportOrder[i] = i
	}
	// Keep the silent Region set stable across rounds without biasing it toward
	// a contiguous key range.
	rs.rng.Shuffle(len(rs.reportOrder), func(i, j int) {
		rs.reportOrder[i], rs.reportOrder[j] = rs.reportOrder[j], rs.reportOrder[i]
	})

	// Generate regions
	id := uint64(1)

	for i := range regionCount {
		region := &pdpb.RegionHeartbeatRequest{
			Header: header,
			Region: &metapb.Region{
				Id:          id,
				StartKey:    codec.GenerateTableKey(int64(i)),
				EndKey:      codec.GenerateTableKey(int64(i + 1)),
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: rs.maxVersion},
			},
			ApproximateSize: rs.regionSize,
			Interval: &pdpb.TimeInterval{
				StartTimestamp: now - regionReportInterval,
				EndTimestamp:   now,
			},
			QueryStats:      &pdpb.QueryStats{},
			ApproximateKeys: rs.regionKeys,
			Term:            1,
		}
		id += 1
		if i == 0 {
			region.Region.StartKey = []byte("")
		}
		if i == regionCount-1 {
			region.Region.EndKey = []byte("")
		}

		peers := make([]*metapb.Peer, 0, replicaCount)
		for j := range replicaCount {
			peers = append(peers, &metapb.Peer{Id: id, StoreId: uint64((i+j)%storeCount + 1)})
			id += 1
		}

		region.Region.Peers = peers
		region.Leader = peers[0]
		rs.Regions = append(rs.Regions, region)
	}
	return rs
}

// Update updates the regions with the given options.
func (rs *Regions) Update(options *config.Options) {
	rs.UpdateRound += 1
	workload := options.Snapshot()

	reportCount := ratioCount(rs.regionCount, workload.ReportRatio)
	reportRegions := append([]int(nil), rs.reportOrder[:reportCount]...)
	// Every ratio uses the total Region count as its denominator. Update sets
	// are selected from reported Regions, so updated Regions are always awake.
	rs.updateFlow = pickCount(reportRegions, ratioCount(rs.regionCount, workload.FlowUpdateRatio))
	rs.updateLeader = rs.randomPickCount(reportRegions, ratioCount(rs.regionCount, workload.LeaderUpdateRatio))
	rs.updateEpoch = rs.randomPickCount(reportRegions, ratioCount(rs.regionCount, workload.EpochUpdateRatio))
	rs.updateSpace = rs.randomPickCount(reportRegions, ratioCount(rs.regionCount, workload.SpaceUpdateRatio))
	var (
		updatedStatisticsMap = make(map[int]*pdpb.RegionHeartbeatRequest)
		awakenRegions        = make([]*pdpb.RegionHeartbeatRequest, 0, reportCount)
	)

	// update leader
	for _, i := range rs.updateLeader {
		region := rs.Regions[i]
		for peerIndex, peer := range region.Region.Peers {
			if peer.GetId() == region.Leader.GetId() {
				region.Leader = region.Region.Peers[(peerIndex+1)%rs.replicaCount]
				region.Term++
				break
			}
		}
	}
	// update epoch
	for _, i := range rs.updateEpoch {
		region := rs.Regions[i]
		region.Region.RegionEpoch.Version += 1
		if region.Region.RegionEpoch.Version > rs.maxVersion {
			rs.maxVersion = region.Region.RegionEpoch.Version
		}
	}
	// update space
	for _, i := range rs.updateSpace {
		region := rs.Regions[i]
		region.ApproximateSize = varyAround(rs.rng, rs.regionSize)
		region.ApproximateKeys = varyAround(rs.rng, rs.regionKeys)
	}
	// update flow
	for _, i := range rs.updateFlow {
		region := rs.Regions[i]
		if region.Leader.StoreId <= uint64(workload.HotStoreCount) {
			region.BytesWritten = uint64(hotByteUnit * (1 + rs.rng.Float64()) * regionReportInterval)
			region.BytesRead = uint64(hotByteUnit * (1 + rs.rng.Float64()) * regionReportInterval)
			region.KeysWritten = uint64(hotKeysUint * (1 + rs.rng.Float64()) * regionReportInterval)
			region.KeysRead = uint64(hotKeysUint * (1 + rs.rng.Float64()) * regionReportInterval)
			region.QueryStats = &pdpb.QueryStats{
				Get: uint64(hotQueryUnit * (1 + rs.rng.Float64()) * regionReportInterval),
				Put: uint64(hotQueryUnit * (1 + rs.rng.Float64()) * regionReportInterval),
			}
		} else {
			region.BytesWritten = uint64(coldByteUnit * rs.rng.Float64())
			region.BytesRead = uint64(coldByteUnit * rs.rng.Float64())
			region.KeysWritten = uint64(coldKeyUnit * rs.rng.Float64())
			region.KeysRead = uint64(coldKeyUnit * rs.rng.Float64())
			region.QueryStats = &pdpb.QueryStats{
				Get: uint64(queryUnit * rs.rng.Float64()),
				Put: uint64(queryUnit * rs.rng.Float64()),
			}
		}
		updatedStatisticsMap[i] = region
	}
	for _, i := range reportRegions {
		region := rs.Regions[i]
		// reset the statistics of the region which is not updated
		if _, exist := updatedStatisticsMap[i]; !exist {
			region.BytesWritten = 0
			region.BytesRead = 0
			region.KeysWritten = 0
			region.KeysRead = 0
			region.QueryStats = &pdpb.QueryStats{}
		}
		awakenRegions = append(awakenRegions, region)
	}

	rs.AwakenRegions.Store(awakenRegions)
}

// PrepareReportIntervals prepares Region heartbeat payloads for the actual round
// start time. Update generates nominal 60-second flow counters; store statistics
// must consume them before this method scales them to the elapsed active round.
// A waking Region's interval includes its silent rounds, without adding traffic
// for that silent time.
func (rs *Regions) PrepareReportIntervals(reportTime time.Time) {
	endTimestamp := uint64(reportTime.Unix())
	elapsed := endTimestamp - min(rs.lastReportTimestamp, endTimestamp)
	for _, region := range rs.ReportedRegions() {
		if rs.UpdateRound == 0 {
			region.Interval.StartTimestamp = endTimestamp - min(endTimestamp, uint64(regionReportInterval))
			region.Interval.EndTimestamp = endTimestamp
			continue
		}
		region.Interval.StartTimestamp = min(region.Interval.EndTimestamp, endTimestamp)
		region.Interval.EndTimestamp = endTimestamp
		region.BytesWritten = region.BytesWritten * elapsed / regionReportInterval
		region.BytesRead = region.BytesRead * elapsed / regionReportInterval
		region.KeysWritten = region.KeysWritten * elapsed / regionReportInterval
		region.KeysRead = region.KeysRead * elapsed / regionReportInterval
		region.QueryStats.Get = region.QueryStats.Get * elapsed / regionReportInterval
		region.QueryStats.Put = region.QueryStats.Put * elapsed / regionReportInterval
	}
	rs.lastReportTimestamp = endTimestamp
}

func ratioCount(total int, ratio float64) int {
	return int(float64(total) * ratio)
}

func (rs *Regions) randomPickCount(indexes []int, count int) []int {
	shuffled := append([]int(nil), indexes...)
	rs.rng.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	return shuffled[:count]
}

func pickCount(indexes []int, count int) []int {
	return append([]int(nil), indexes[:count]...)
}

func varyAround(rng *rand.Rand, base uint64) uint64 {
	value := uint64(float64(base) * (0.5 + rng.Float64()))
	if value == base {
		return value + 1
	}
	return value
}

// ReportedRegions returns the Regions that report in the current round.
func (rs *Regions) ReportedRegions() []*pdpb.RegionHeartbeatRequest {
	reported := rs.AwakenRegions.Load()
	if reported == nil {
		return rs.Regions
	}
	return reported.([]*pdpb.RegionHeartbeatRequest)
}

// GroupReportedRegionsByLeader groups current reporting Regions in one scan.
func (rs *Regions) GroupReportedRegionsByLeader(storeCount int) [][]*pdpb.RegionHeartbeatRequest {
	groups := make([][]*pdpb.RegionHeartbeatRequest, storeCount+1)
	for _, region := range rs.ReportedRegions() {
		storeID := region.GetLeader().GetStoreId()
		if storeID > 0 && storeID <= uint64(storeCount) {
			groups[storeID] = append(groups[storeID], region)
		}
	}
	return groups
}

// MaxVersion returns the largest generated Region epoch version.
func (rs *Regions) MaxVersion() uint64 {
	return rs.maxVersion
}

// HandleRegionHeartbeat sends one store's Region heartbeats. The measured
// duration is client-side stream send/backpressure time, not PD processing
// latency. Use PD's server-side metrics for processing latency.
func (*Regions) HandleRegionHeartbeat(
	ctx context.Context,
	wg *sync.WaitGroup,
	stream pdpb.PD_RegionHeartbeatClient,
	storeID uint64,
	regions []*pdpb.RegionHeartbeatRequest,
	rep report.Report,
) {
	defer wg.Done()
	batchStart := time.Now()
	var err error
	for _, region := range regions {
		if ctx.Err() != nil {
			return
		}
		start := time.Now()
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
	log.Info("store finished one round of region heartbeat sends",
		zap.Uint64("store-id", storeID),
		zap.Duration("send-duration", time.Since(batchStart)),
		zap.Int("reported-region-count", len(regions)))
}

// Result prints the result of the region heartbeat.
func (rs *Regions) Result(sec float64) {
	if rs.UpdateRound == 0 {
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
	reportedCount := len(rs.ReportedRegions())
	unchangedReportedCount := reportedCount - len(updated)
	silentCount := rs.regionCount - reportedCount

	log.Info("region heartbeat workload rates",
		zap.String("reported-rps", fmt.Sprintf("%.4f", float64(reportedCount)/sec)),
		zap.String("leader-update-rps", fmt.Sprintf("%.4f", float64(len(rs.updateLeader))/sec)),
		zap.String("epoch-update-rps", fmt.Sprintf("%.4f", float64(len(rs.updateEpoch))/sec)),
		zap.String("space-update-rps", fmt.Sprintf("%.4f", float64(len(rs.updateSpace))/sec)),
		zap.String("flow-update-rps", fmt.Sprintf("%.4f", float64(len(rs.updateFlow))/sec)),
		zap.Int("reported-unchanged-count", unchangedReportedCount),
		zap.Int("silent-count", silentCount))
}
