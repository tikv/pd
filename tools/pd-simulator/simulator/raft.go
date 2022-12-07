// Copyright 2018 TiKV Project Authors.
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

package simulator

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tools/pd-simulator/simulator/cases"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

// RaftEngine records all raft information.
type RaftEngine struct {
	syncutil.RWMutex
	regionsInfo       *core.RegionsInfo
	conn              *Connection
	regionChangeMu    syncutil.RWMutex
	regionChange      map[uint64][]*core.RegionInfo
	regionSplitSize   int64
	regionSplitKeys   int64
	storeConfig       *SimConfig
	useTiDBEncodedKey bool
	cachedRegions     []*core.RegionInfo
	changeMu          syncutil.RWMutex
	toDelete          map[uint64]*core.RegionInfo
	toAdd             map[uint64]*core.RegionInfo
}

// NewRaftEngine creates the initialized raft with the configuration.
func NewRaftEngine(conf *cases.Case, conn *Connection, storeConfig *SimConfig) *RaftEngine {
	r := &RaftEngine{
		regionsInfo:     core.NewRegionsInfo(),
		conn:            conn,
		regionChange:    make(map[uint64][]*core.RegionInfo),
		regionSplitSize: conf.RegionSplitSize,
		regionSplitKeys: conf.RegionSplitKeys,
		storeConfig:     storeConfig,
		toDelete:        make(map[uint64]*core.RegionInfo),
		toAdd:           make(map[uint64]*core.RegionInfo),
	}
	var splitKeys []string
	if conf.TableNumber > 0 {
		splitKeys = simutil.GenerateTableKeys(conf.TableNumber, len(conf.Regions)-1)
		r.useTiDBEncodedKey = true
	} else {
		splitKeys = simutil.GenerateKeys(len(conf.Regions) - 1)
	}

	for i, region := range conf.Regions {
		meta := &metapb.Region{
			Id:          region.ID,
			Peers:       region.Peers,
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		}
		if i > 0 {
			meta.StartKey = []byte(splitKeys[i-1])
		}
		if i < len(conf.Regions)-1 {
			meta.EndKey = []byte(splitKeys[i])
		}
		regionSize := storeConfig.Coprocessor.RegionSplitSize
		regionInfo := core.NewRegionInfo(
			meta,
			region.Leader,
			core.SetApproximateSize(int64(regionSize)),
			core.SetApproximateKeys(int64(storeConfig.Coprocessor.RegionSplitKey)),
		)
		r.SetRegion(regionInfo)
		peers := region.Peers
		for _, peer := range peers {
			r.conn.Nodes[peer.StoreId].incUsedSize(uint64(regionSize))
		}
	}

	for _, node := range conn.Nodes {
		node.raftEngine = r
	}
	r.cachedRegions = r.regionsInfo.GetRegions()

	return r
}

func (r *RaftEngine) stepRegions() {
	if len(r.toDelete) == 0 && len(r.toAdd) == 0 {
		for _, region := range r.cachedRegions {
			r.stepLeader(region)
			r.stepSplit(region)
		}
	} else {
		r.changeMu.Lock()
		for i, region := range r.cachedRegions {
			id := region.GetID()
			if _, ok := r.toDelete[id]; ok {
				r.cachedRegions = append(r.cachedRegions[:i], r.cachedRegions[:i+1]...)
				delete(r.toDelete, id)
				continue
			}
		}
		for _, toAdd := range r.toAdd {
			r.cachedRegions = append(r.cachedRegions, toAdd)
		}
		r.changeMu.Unlock()
		for _, region := range r.cachedRegions {
			r.stepLeader(region)
			r.stepSplit(region)
		}
	}
}

func (r *RaftEngine) stepLeader(region *core.RegionInfo) {
	// TODO: add check node health back
	if region.GetLeader() != nil {
		return
	}
	newLeader := r.electNewLeader(region)
	newRegion := region.Clone(core.WithLeader(newLeader))
	if newLeader == nil {
		r.changeMu.Lock()
		overlaps := r.SetRegion(newRegion)
		r.toAdd[newRegion.GetID()] = newRegion
		for _, overlap := range overlaps {
			r.toDelete[overlap.GetID()] = overlap
		}
		r.changeMu.Unlock()
		simutil.Logger.Info("region has no leader", zap.Uint64("region-id", region.GetID()))
		return
	}
	simutil.Logger.Info("region elects a new leader",
		zap.Uint64("region-id", region.GetID()),
		zap.Reflect("new-leader", newLeader),
		zap.Reflect("old-leader", region.GetLeader()))
	r.changeMu.Lock()
	overlaps := r.SetRegion(newRegion)
	r.toAdd[newRegion.GetID()] = newRegion
	for _, overlap := range overlaps {
		r.toDelete[overlap.GetID()] = overlap
	}
	r.changeMu.Unlock()
	r.recordRegionChange(newRegion)
}

func (r *RaftEngine) stepSplit(region *core.RegionInfo) {
	if region.GetLeader() == nil {
		return
	}
	if !r.NeedSplit(region.GetApproximateSize(), region.GetApproximateKeys()) {
		return
	}
	var err error
	ids := make([]uint64, 1+len(region.GetPeers()))
	for i := range ids {
		ids[i], err = r.allocID(region.GetLeader().GetStoreId())
		if err != nil {
			simutil.Logger.Error("alloc id failed", zap.Error(err))
			return
		}
	}

	var splitKey []byte
	if r.useTiDBEncodedKey {
		splitKey, err = simutil.GenerateTiDBEncodedSplitKey(region.GetStartKey(), region.GetEndKey())
		if err != nil {
			simutil.Logger.Fatal("Generate TiDB encoded split key failed", zap.Error(err))
		}
	} else {
		splitKey = simutil.GenerateSplitKey(region.GetStartKey(), region.GetEndKey())
	}
	left := region.Clone(
		core.WithNewRegionID(ids[len(ids)-1]),
		core.WithNewPeerIDs(ids[0:len(ids)-1]...),
		core.WithIncVersion(),
		core.SetApproximateKeys(region.GetApproximateKeys()/2),
		core.SetApproximateSize(region.GetApproximateSize()/2),
		core.WithPendingPeers(nil),
		core.WithDownPeers(nil),
		core.WithEndKey(splitKey),
	)
	right := region.Clone(
		core.WithIncVersion(),
		core.SetApproximateKeys(region.GetApproximateKeys()/2),
		core.SetApproximateSize(region.GetApproximateSize()/2),
		core.WithStartKey(splitKey),
	)

	r.changeMu.Lock()
	overlaps := r.SetRegion(right)
	r.toAdd[right.GetID()] = right
	r.toAdd[left.GetID()] = left
	for _, overlap := range overlaps {
		r.toDelete[overlap.GetID()] = overlap
	}
	overlaps = r.SetRegion(left)
	for _, overlap := range overlaps {
		r.toDelete[overlap.GetID()] = overlap
	}
	r.changeMu.Unlock()
	simutil.Logger.Debug("region split",
		zap.Uint64("region-id", region.GetID()),
		zap.Reflect("origin", region.GetMeta()),
		zap.Reflect("left", left.GetMeta()),
		zap.Reflect("right", right.GetMeta()))
	r.recordRegionChange(left)
	r.recordRegionChange(right)
}

// NeedSplit checks whether the region needs to split according its size
// and number of keys.
func (r *RaftEngine) NeedSplit(size, rows int64) bool {
	if r.regionSplitSize != 0 && size >= r.regionSplitSize {
		return true
	}
	if r.regionSplitKeys != 0 && rows >= r.regionSplitKeys {
		return true
	}
	return false
}

func (r *RaftEngine) recordRegionChange(region *core.RegionInfo) {
	r.regionChangeMu.Lock()
	defer r.regionChangeMu.Unlock()
	n := region.GetLeader().GetStoreId()
	r.regionChange[n] = append(r.regionChange[n], region)
}

func (r *RaftEngine) updateRegionStore(region *core.RegionInfo, size int64) {
	newRegion := region.Clone(
		core.SetApproximateSize(region.GetApproximateSize()+size),
		core.SetWrittenBytes(uint64(size)),
	)
	storeIDs := region.GetStoreIDs()
	for storeID := range storeIDs {
		r.conn.Nodes[storeID].incUsedSize(uint64(size))
	}
	r.changeMu.Lock()
	overlaps := r.SetRegion(newRegion)
	r.toAdd[newRegion.GetID()] = newRegion
	for _, overlap := range overlaps {
		r.toDelete[overlap.GetID()] = overlap
	}
	r.changeMu.Unlock()
}

func (r *RaftEngine) updateRegionReadBytes(readBytes map[uint64]int64) {
	for id, bytes := range readBytes {
		region := r.GetRegion(id)
		if region == nil {
			simutil.Logger.Error("region is not found", zap.Uint64("region-id", id))
			continue
		}
		newRegion := region.Clone(core.SetReadBytes(uint64(bytes)))
		r.changeMu.Lock()
		overlaps := r.SetRegion(newRegion)
		r.toAdd[newRegion.GetID()] = newRegion
		for _, overlap := range overlaps {
			r.toDelete[overlap.GetID()] = overlap
		}
		r.changeMu.Unlock()
	}
}

func (r *RaftEngine) electNewLeader(region *core.RegionInfo) *metapb.Peer {
	var (
		unhealthy        int
		newLeaderStoreID uint64
	)
	ids := region.GetStoreIDs()
	for id := range ids {
		if r.conn.nodeHealth(id) {
			newLeaderStoreID = id
		} else {
			unhealthy++
		}
	}
	if unhealthy > len(ids)/2 {
		return nil
	}
	for _, peer := range region.GetPeers() {
		if peer.GetStoreId() == newLeaderStoreID {
			return peer
		}
	}
	return nil
}

// GetRegion returns the RegionInfo with regionID.
func (r *RaftEngine) GetRegion(regionID uint64) *core.RegionInfo {
	r.RLock()
	defer r.RUnlock()
	return r.regionsInfo.GetRegion(regionID)
}

// GetRegionChange returns a list of RegionID for a given store.
func (r *RaftEngine) GetRegionChange(storeID uint64) []*core.RegionInfo {
	r.regionChangeMu.Lock()
	defer r.regionChangeMu.Unlock()
	return r.regionChange[storeID]
}

// ResetRegionChange resets RegionInfo on a specific store with a given Region ID
func (r *RaftEngine) ResetRegionChange(storeID uint64, regionID uint64) {
	r.regionChangeMu.Lock()
	defer r.regionChangeMu.Unlock()
	regions := r.regionChange[storeID]
	for i, region := range regions {
		if region.GetID() == regionID {
			r.regionChange[storeID] = append(r.regionChange[storeID][:i], r.regionChange[storeID][i+1:]...)
			return
		}
	}
}

// GetRegions gets all RegionInfo from regionMap
func (r *RaftEngine) GetRegions() []*core.RegionInfo {
	r.RLock()
	defer r.RUnlock()
	return r.regionsInfo.GetRegions()
}

// SetRegion sets the RegionInfo with regionID
func (r *RaftEngine) SetRegion(region *core.RegionInfo) []*core.RegionInfo {
	r.Lock()
	defer r.Unlock()
	return r.regionsInfo.SetRegion(region)
}

// GetRegionByKey searches the RegionInfo from regionTree
func (r *RaftEngine) GetRegionByKey(regionKey []byte) *core.RegionInfo {
	r.RLock()
	defer r.RUnlock()
	return r.regionsInfo.GetRegionByKey(regionKey)
}

// BootstrapRegion gets a region to construct bootstrap info.
func (r *RaftEngine) BootstrapRegion() *core.RegionInfo {
	r.RLock()
	defer r.RUnlock()
	regions := r.regionsInfo.ScanRange(nil, nil, 1)
	if len(regions) > 0 {
		return regions[0]
	}
	return nil
}

func (r *RaftEngine) allocID(storeID uint64) (uint64, error) {
	node, ok := r.conn.Nodes[storeID]
	if !ok {
		return 0, errors.Errorf("node %d not found", storeID)
	}
	id, err := node.client.AllocID(context.Background())
	return id, errors.WithStack(err)
}
