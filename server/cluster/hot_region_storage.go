// Copyright 2021 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/encryptionkm"
	"github.com/tikv/pd/server/kv"
	"github.com/tikv/pd/server/member"
	"github.com/tikv/pd/server/statistics"
)

// HotRegionStorage is used to storage hot region info,
// It will pull the hot region information according to the pullInterval interval.
// And delete and save data beyond the remainingDays.
// Close must be called after use.
type HotRegionStorage struct {
	*kv.LeveldbKV
	encryptionKeyManager *encryptionkm.KeyManager
	mu                   sync.RWMutex
	batchHotInfo         map[string]*statistics.HistoryHotRegion
	remianedDays         int64
	pullInterval         time.Duration
	compactionCountdown  int
	hotRegionInfoCtx     context.Context
	hotRegionInfoCancel  context.CancelFunc
	cluster              *RaftCluster
	member               *member.Member
}

const (
	// leveldb will run compaction after 30 times delete.
	defaultCompactionTime = 30
	// delete will run at this o`clock.
	defaultDeleteTime = 4
)

// HotRegionTypes stands for hot type.
var HotRegionTypes = []string{
	"read",
	"write",
}

// NewHotRegionsStorage create storage to store hot regions info.
func NewHotRegionsStorage(
	ctx context.Context,
	path string,
	encryptionKeyManager *encryptionkm.KeyManager,
	cluster *RaftCluster,
	member *member.Member,
	remianedDays int64,
	pullInterval time.Duration,
) (*HotRegionStorage, error) {
	levelDB, err := kv.NewLeveldbKV(path)
	if err != nil {
		return nil, err
	}
	hotRegionInfoCtx, hotRegionInfoCancle := context.WithCancel(ctx)
	h := HotRegionStorage{
		LeveldbKV:            levelDB,
		encryptionKeyManager: encryptionKeyManager,
		batchHotInfo:         make(map[string]*statistics.HistoryHotRegion),
		remianedDays:         remianedDays,
		pullInterval:         pullInterval,
		compactionCountdown:  defaultCompactionTime,
		hotRegionInfoCtx:     hotRegionInfoCtx,
		hotRegionInfoCancel:  hotRegionInfoCancle,
		cluster:              cluster,
		member:               member,
	}
	if remianedDays > 0 {
		h.backgroundFlush()
		h.backgroundDelete()
	}
	return &h, nil
}

// delete hot region which update_time is smaller than time.Now() minus remain day in the background.
func (h *HotRegionStorage) backgroundDelete() {
	// make delete happened in defaultDeleteTime clock.
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day(), 12, 0, 0, 0, now.Location())
	d := next.Sub(now)
	if d < 0 {
		d = d + 24*time.Hour
	}
	ticker := time.NewTicker(d)
	go func() {
		defer ticker.Stop()
		select {
		case <-ticker.C:
			ticker.Reset(24 * time.Hour)
			h.delete()
		case <-h.hotRegionInfoCtx.Done():
			return
		}
		for {
			select {
			case <-ticker.C:
				h.delete()
			case <-h.hotRegionInfoCtx.Done():
				return
			}
		}
	}()
}

// Write hot_region info into db in the background.
func (h *HotRegionStorage) backgroundFlush() {
	ticker := time.NewTicker(h.pullInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if h.member.IsLeader() {
					if err := h.pullHotRegionInfo(); err != nil {
						log.Error("get hot_region stat meet error", errs.ZapError(err))
					}
					if err := h.flush(); err != nil {
						log.Error("get hot_region stat meet error", errs.ZapError(err))
					}
				}
			case <-h.hotRegionInfoCtx.Done():
				return
			}
		}
	}()
}

// NewIterator return a iterator which can traverse all data as reqeust.
func (h *HotRegionStorage) NewIterator(requireTypes []string, startTime, endTime int64) HotRegionStorageIterator {
	iters := make([]iterator.Iterator, len(requireTypes))
	for index, requireType := range requireTypes {
		startKey := HotRegionStorePath(requireType, startTime, 0)
		endKey := HotRegionStorePath(requireType, endTime, math.MaxInt64)
		iter := h.LeveldbKV.NewIterator(&util.Range{Start: []byte(startKey), Limit: []byte(endKey)}, nil)
		iters[index] = iter
	}
	return HotRegionStorageIterator{
		iters:                iters,
		encryptionKeyManager: h.encryptionKeyManager,
	}
}

// Close closes the kv.
func (h *HotRegionStorage) Close() error {
	h.hotRegionInfoCancel()
	if err := h.LeveldbKV.Close(); err != nil {
		return errs.ErrLevelDBClose.Wrap(err).GenWithStackByArgs()
	}
	return nil
}

func (h *HotRegionStorage) pullHotRegionInfo() error {
	cluster := h.cluster
	hotReadRegion := cluster.coordinator.getHotReadRegions()
	hotReadLeaderRegion := hotReadRegion.AsLeader
	hotReadPeerRegion := hotReadRegion.AsPeer
	if err := h.packHotRegionInfo(hotReadLeaderRegion,
		HotRegionTypes[0], false); err != nil {
		return err
	}
	if err := h.packHotRegionInfo(hotReadPeerRegion,
		HotRegionTypes[0], true); err != nil {
		return err
	}
	hotWriteRegion := cluster.coordinator.getHotWriteRegions()
	hotWriteLeaderInfo := hotWriteRegion.AsLeader
	hotWritePeerInfo := hotWriteRegion.AsPeer
	if err := h.packHotRegionInfo(hotWriteLeaderInfo,
		HotRegionTypes[1], true); err != nil {
		return err
	}
	err := h.packHotRegionInfo(hotWritePeerInfo,
		HotRegionTypes[1], false)
	return err
}

func (h *HotRegionStorage) packHotRegionInfo(hotLeaderInfo statistics.StoreHotPeersStat,
	hotRegionType string, isLeader bool) error {
	cluster := h.cluster
	batchHotInfo := h.batchHotInfo
	for _, hotPeersStat := range hotLeaderInfo {
		stats := hotPeersStat.Stats
		for _, hotPeerStat := range stats {
			region := cluster.GetRegion(hotPeerStat.RegionID).GetMeta()
			region, err := encryption.EncryptRegion(region, h.encryptionKeyManager)
			if err != nil {
				return err
			}
			var peerID uint64
			for _, peer := range region.Peers {
				if peer.StoreId == hotPeerStat.StoreID {
					peerID = peer.Id
				}
			}
			stat := statistics.HistoryHotRegion{
				// store in  ms.
				UpdateTime:     hotPeerStat.LastUpdateTime.UnixNano() / int64(time.Millisecond),
				RegionID:       hotPeerStat.RegionID,
				StoreID:        hotPeerStat.StoreID,
				PeerID:         peerID,
				IsLeader:       isLeader,
				HotDegree:      int64(hotPeerStat.HotDegree),
				FlowBytes:      hotPeerStat.ByteRate,
				KeyRate:        hotPeerStat.KeyRate,
				QueryRate:      hotPeerStat.QueryRate,
				StartKey:       region.StartKey,
				EndKey:         region.EndKey,
				EncryptionMeta: region.EncryptionMeta,
				HotRegionType:  hotRegionType,
			}
			batchHotInfo[HotRegionStorePath(
				stat.HotRegionType,
				// store in ms.
				hotPeerStat.LastUpdateTime.UnixNano()/int64(time.Millisecond),
				hotPeerStat.RegionID)] = &stat
		}
	}
	return nil
}

func (h *HotRegionStorage) flush() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	batch := new(leveldb.Batch)
	for key, stat := range h.batchHotInfo {
		value, err := json.Marshal(stat)
		if err != nil {
			return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
		}
		batch.Put([]byte(key), value)
	}
	if err := h.LeveldbKV.Write(batch, nil); err != nil {
		return errs.ErrLevelDBWrite.Wrap(err).GenWithStackByCause()
	}
	h.batchHotInfo = make(map[string]*statistics.HistoryHotRegion)
	return nil
}

func (h *HotRegionStorage) delete() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	db := h.LeveldbKV
	batch := new(leveldb.Batch)
	for _, hotRegionType := range HotRegionTypes {
		startKey := HotRegionStorePath(hotRegionType, 0, 0)
		endTime := time.Now().AddDate(0, 0, 0-int(h.remianedDays)).UnixNano() / int64(time.Millisecond)
		endKey := HotRegionStorePath(hotRegionType, endTime, math.MaxInt64)
		iter := db.NewIterator(&util.Range{
			Start: []byte(startKey), Limit: []byte(endKey)}, nil)
		for iter.Next() {
			batch.Delete(iter.Key())
		}
	}
	if err := db.Write(batch, nil); err != nil {
		return errs.ErrLevelDBWrite.Wrap(err).GenWithStackByCause()
	}
	h.compactionCountdown--
	if h.compactionCountdown == 0 {
		h.compactionCountdown = defaultCompactionTime
		for _, hotRegionType := range HotRegionTypes {
			startKey := HotRegionStorePath(hotRegionType, 0, 0)
			endTime := time.Now().AddDate(0, 0, 0-int(h.remianedDays)).Unix()
			endKey := HotRegionStorePath(hotRegionType, endTime, math.MaxInt64)
			db.CompactRange(util.Range{Start: []byte(startKey), Limit: []byte(endKey)})
		}
	}
	return nil
}

// HotRegionStorageIterator iterates over a historyHotRegion.
type HotRegionStorageIterator struct {
	iters                []iterator.Iterator
	encryptionKeyManager *encryptionkm.KeyManager
}

// Next moves the iterator to the next key/value pair.
// And return historyHotRegion which it is now pointing to.
// it will return (nil,nil),if there is no more historyHotRegion.
func (it *HotRegionStorageIterator) Next() (*statistics.HistoryHotRegion, error) {
	iter := it.iters[0]
	for !iter.Next() {
		iter.Release()
		if len(it.iters) == 1 {
			return nil, nil
		}
		it.iters = it.iters[1:]
		iter = it.iters[0]
	}
	item := iter.Value()
	value := make([]byte, len(item))
	copy(value, item)
	var message statistics.HistoryHotRegion
	err := json.Unmarshal(value, &message)
	if err != nil {
		return nil, err
	}
	region := &metapb.Region{
		Id:             message.RegionID,
		StartKey:       message.StartKey,
		EndKey:         message.EndKey,
		EncryptionMeta: message.EncryptionMeta,
	}
	if err := encryption.DecryptRegion(region, it.encryptionKeyManager); err != nil {
		return nil, err
	}
	message.StartKey = region.StartKey
	message.EndKey = region.EndKey
	message.EncryptionMeta = nil
	return &message, nil
}

// HotRegionStorePath generate hot region store key for HotRegionStorage.
// TODO:find a better place to put this function.
func HotRegionStorePath(hotRegionType string, updateTime int64, regionID uint64) string {
	return path.Join(
		"schedule",
		"hot_region",
		hotRegionType,
		fmt.Sprintf("%020d", updateTime),
		fmt.Sprintf("%020d", regionID),
	)
}
