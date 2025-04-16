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
	"slices"
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
		Address: fmt.Sprintf("localhost:%d", 2),
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
					defer cancel()
					_, _ = cli.StoreHeartbeat(cctx, &pdpb.StoreHeartbeatRequest{
						Header: header,
						Stats: &pdpb.StoreStats{
							StoreId: storeID,
						},
					})
				case <-ctx.Done():
					return
				}
			}
		}(ctx, storeID)
	}
}

const (
	bytesUnit            = 128
	keysUint             = 8
	queryUnit            = 8
	hotByteUnit          = 16 * units.KiB
	hotKeysUint          = 256
	hotQueryUnit         = 256
	regionReportInterval = 60 // 60s
)

// Regions simulates all regions to heartbeat.
type Regions struct {
	regionCount  int
	replicaCount int
	maxVersion   uint64
	// Regions is the list of all regions to heartbeat.
	Regions []*pdpb.RegionHeartbeatRequest
	// AwakenRegions is the number of regions to awaken.
	AwakenRegions atomic.Value

	UpdateRound int

	updateLeader []int
	updateEpoch  []int
	updateSpace  []int
	updateFlow   []int
}

// NewRegions initializes the regions with the given region count and replica count.
func NewRegions(regionCount, replicaCount int, header *pdpb.RequestHeader) *Regions {
	rs := &Regions{
		regionCount:  regionCount,
		replicaCount: replicaCount,
		Regions:      make([]*pdpb.RegionHeartbeatRequest, 0, regionCount),
		UpdateRound:  0,
		maxVersion:   1,
	}

	// Generate regions
	id := uint64(1)
	now := uint64(time.Now().Unix())

	for i := range regionCount {
		region := &pdpb.RegionHeartbeatRequest{
			Header: header,
			Region: &metapb.Region{
				Id:          id,
				StartKey:    codec.GenerateTableKey(int64(i)),
				EndKey:      codec.GenerateTableKey(int64(i + 1)),
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: rs.maxVersion},
			},
			ApproximateSize: bytesUnit,
			Interval: &pdpb.TimeInterval{
				StartTimestamp: now,
				EndTimestamp:   now + regionReportInterval,
			},
			QueryStats:      &pdpb.QueryStats{},
			ApproximateKeys: keysUint,
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
			peers = append(peers, &metapb.Peer{Id: id, StoreId: uint64((i+j)%replicaCount + 1)})
			id += 1
		}

		region.Region.Peers = peers
		region.Leader = peers[0]
		rs.Regions = append(rs.Regions, region)
	}
	return rs
}

// Update updates the regions with the given options.
func (rs *Regions) Update(regionCount, replicaCount int, options *config.Options) {
	rs.UpdateRound += 1

	// Generate sample index
	indexes := make([]int, regionCount)
	for i := range indexes {
		indexes[i] = i
	}
	reportRegions := pick(indexes, regionCount, options.GetReportRatio())

	reportCount := len(reportRegions)
	rs.updateFlow = pick(reportRegions, reportCount, options.GetFlowUpdateRatio())
	rs.updateLeader = randomPick(reportRegions, reportCount, options.GetLeaderUpdateRatio())
	rs.updateEpoch = randomPick(reportRegions, reportCount, options.GetEpochUpdateRatio())
	rs.updateSpace = randomPick(reportRegions, reportCount, options.GetSpaceUpdateRatio())
	var (
		updatedStatisticsMap = make(map[int]*pdpb.RegionHeartbeatRequest)
		awakenRegions        []*pdpb.RegionHeartbeatRequest
	)

	// update leader
	for _, i := range rs.updateLeader {
		region := rs.Regions[i]
		region.Leader = region.Region.Peers[rs.UpdateRound%replicaCount]
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
		region.ApproximateSize = uint64(bytesUnit * rand.Float64())
		region.ApproximateKeys = uint64(keysUint * rand.Float64())
	}
	// update flow
	for _, i := range rs.updateFlow {
		region := rs.Regions[i]
		if region.Leader.StoreId <= uint64(options.GetHotStoreCount()) {
			region.BytesWritten = uint64(hotByteUnit * (1 + rand.Float64()) * 60)
			region.BytesRead = uint64(hotByteUnit * (1 + rand.Float64()) * 10)
			region.KeysWritten = uint64(hotKeysUint * (1 + rand.Float64()) * 60)
			region.KeysRead = uint64(hotKeysUint * (1 + rand.Float64()) * 10)
			region.QueryStats = &pdpb.QueryStats{
				Get: uint64(hotQueryUnit * (1 + rand.Float64()) * 10),
				Put: uint64(hotQueryUnit * (1 + rand.Float64()) * 60),
			}
		} else {
			region.BytesWritten = uint64(bytesUnit * rand.Float64())
			region.BytesRead = uint64(bytesUnit * rand.Float64())
			region.KeysWritten = uint64(keysUint * rand.Float64())
			region.KeysRead = uint64(keysUint * rand.Float64())
			region.QueryStats = &pdpb.QueryStats{
				Get: uint64(queryUnit * rand.Float64()),
				Put: uint64(queryUnit * rand.Float64()),
			}
		}
		updatedStatisticsMap[i] = region
	}
	// update interval
	for _, region := range rs.Regions {
		region.Interval.StartTimestamp = region.Interval.EndTimestamp
		region.Interval.EndTimestamp = region.Interval.StartTimestamp + regionReportInterval
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

func randomPick(slice []int, total int, ratio float64) []int {
	rand.Shuffle(total, func(i, j int) {
		slice[i], slice[j] = slice[j], slice[i]
	})
	return slices.Clone(slice[0:int(float64(total)*ratio)])
}

func pick(slice []int, total int, ratio float64) []int {
	return slices.Clone(slice[0:int(float64(total)*ratio)])
}

// HandleRegionHeartbeat handles the region heartbeat for the given store.
func (rs *Regions) HandleRegionHeartbeat(wg *sync.WaitGroup, stream pdpb.PD_RegionHeartbeatClient, storeID uint64, rep report.Report) {
	defer wg.Done()
	var regions, toUpdate []*pdpb.RegionHeartbeatRequest
	updatedRegions := rs.AwakenRegions.Load()
	if updatedRegions == nil {
		toUpdate = rs.Regions
	} else {
		toUpdate = updatedRegions.([]*pdpb.RegionHeartbeatRequest)
	}
	for _, region := range toUpdate {
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
	log.Info("store finish one round region heartbeat", zap.Uint64("store-id", storeID), zap.Duration("cost-time", time.Since(start)), zap.Int("reported-region-count", len(regions)))
}

// Result prints the result of the region heartbeat.
func (rs *Regions) Result(regionCount int, sec float64) {
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
	inactiveCount := regionCount - len(updated)

	log.Info("update speed of each category", zap.String("rps", fmt.Sprintf("%.4f", float64(regionCount)/sec)),
		zap.String("save-tree", fmt.Sprintf("%.4f", float64(len(rs.updateLeader))/sec)),
		zap.String("save-kv", fmt.Sprintf("%.4f", float64(len(rs.updateEpoch))/sec)),
		zap.String("save-space", fmt.Sprintf("%.4f", float64(len(rs.updateSpace))/sec)),
		zap.String("save-flow", fmt.Sprintf("%.4f", float64(len(rs.updateFlow))/sec)),
		zap.String("skip", fmt.Sprintf("%.4f", float64(inactiveCount)/sec)))
}
