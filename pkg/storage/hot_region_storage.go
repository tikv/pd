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

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.uber.org/zap"
)

// HotRegionStorage is used to store the hot region info.
// It will pull the hot region info according to the `pullInterval`,
// and save/delete data beyond the `remainingDays`.
// Close() must be called after the use.
type HotRegionStorage struct {
	*kv.LevelDBKV
	ekm                    *encryption.Manager
	hotRegionLoopWg        sync.WaitGroup
	batchHotInfo           map[string]*HistoryHotRegion
	hotRegionInfoCtx       context.Context
	hotRegionInfoCancel    context.CancelFunc
	hotRegionStorageHelper HotRegionStorageHelper

	curReservedDays uint64
	curInterval     time.Duration
	mu              syncutil.RWMutex
}

// HistoryHotRegions wraps historyHotRegion
// it will be returned to TiDB.
//
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type HistoryHotRegions struct {
	HistoryHotRegion []*HistoryHotRegion `json:"history_hot_region"`
}

// HistoryHotRegion wraps hot region info
// it is storage format of hot_region_storage
type HistoryHotRegion struct {
	UpdateTime    int64   `json:"update_time"`
	RegionID      uint64  `json:"region_id"`
	PeerID        uint64  `json:"peer_id"`
	StoreID       uint64  `json:"store_id"`
	IsLeader      bool    `json:"is_leader"`
	IsLearner     bool    `json:"is_learner"`
	HotRegionType string  `json:"hot_region_type"`
	HotDegree     int64   `json:"hot_degree"`
	FlowBytes     float64 `json:"flow_bytes"`
	KeyRate       float64 `json:"key_rate"`
	QueryRate     float64 `json:"query_rate"`
	StartKey      string  `json:"start_key"`
	EndKey        string  `json:"end_key"`
	// Encryption metadata for start_key and end_key. encryption_meta.iv is IV for start_key.
	// IV for end_key is calculated from (encryption_meta.iv + len(start_key)).
	// The field is only used by PD and should be ignored otherwise.
	// If encryption_meta is empty (i.e. nil), it means start_key and end_key are unencrypted.
	EncryptionMeta *encryptionpb.EncryptionMeta `json:"encryption_meta,omitempty"`
}

// HotRegionStorageHelper help hot region storage get hot region info.
type HotRegionStorageHelper interface {
	// GetHistoryHotRegions get hot region info in HistoryHotRegion form.
	GetHistoryHotRegions(typ utils.RWType) ([]HistoryHotRegion, error)
	// IsLeader return true means this server is leader.
	IsLeader() bool
	// GetHotRegionsWriteInterval gets interval for PD to store Hot Region information.
	GetHotRegionsWriteInterval() time.Duration
	// GetHotRegionsReservedDays gets days hot region information is kept.
	GetHotRegionsReservedDays() uint64
}

const (
	// delete will run at this o`clock.
	defaultDeleteTime = 4
)

// HotRegionTypes stands for hot type.
var HotRegionTypes = []string{
	utils.Read.String(),
	utils.Write.String(),
}

// NewHotRegionsStorage create storage to store hot regions info.
func NewHotRegionsStorage(
	ctx context.Context,
	filePath string,
	ekm *encryption.Manager,
	hotRegionStorageHelper HotRegionStorageHelper,
) (*HotRegionStorage, error) {
	levelDB, err := kv.NewLevelDBKV(filePath)
	if err != nil {
		return nil, err
	}
	hotRegionInfoCtx, hotRegionInfoCancel := context.WithCancel(ctx)
	h := HotRegionStorage{
		LevelDBKV:              levelDB,
		ekm:                    ekm,
		batchHotInfo:           make(map[string]*HistoryHotRegion),
		hotRegionInfoCtx:       hotRegionInfoCtx,
		hotRegionInfoCancel:    hotRegionInfoCancel,
		hotRegionStorageHelper: hotRegionStorageHelper,
		curReservedDays:        hotRegionStorageHelper.GetHotRegionsReservedDays(),
		curInterval:            hotRegionStorageHelper.GetHotRegionsWriteInterval(),
	}
	h.hotRegionLoopWg.Add(2)
	go h.backgroundFlush()
	go h.backgroundDelete()
	return &h, nil
}

// Delete hot region whose update_time is smaller than time.Now() minus remain day in the background.
func (h *HotRegionStorage) backgroundDelete() {
	defer logutil.LogPanic()

	// make delete happened in defaultDeleteTime clock.
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day(), defaultDeleteTime, 0, 0, 0, now.Location())
	d := next.Sub(now)
	if d < 0 {
		d += 24 * time.Hour
	}
	isFirst := true
	ticker := time.NewTicker(d)
	defer func() {
		ticker.Stop()
		h.hotRegionLoopWg.Done()
	}()
	for {
		select {
		case <-ticker.C:
			h.updateReservedDays()
			curReservedDays := h.getCurReservedDays()
			if isFirst {
				ticker.Reset(24 * time.Hour)
				isFirst = false
			}
			if curReservedDays == 0 {
				log.Warn(`hot region reserved days is 0, if previous reserved days is non 0,
				 there may be residual hot regions, you can remove it manually, [pd-dir]/data/hot-region.`)
				continue
			}
			if err := h.delete(int(curReservedDays)); err != nil {
				log.Error("delete hot region meet error", errs.ZapError(err))
			}
		case <-h.hotRegionInfoCtx.Done():
			return
		}
	}
}

// Write hot_region info into db in the background.
func (h *HotRegionStorage) backgroundFlush() {
	defer logutil.LogPanic()

	interval := h.getCurInterval()
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		h.hotRegionLoopWg.Done()
	}()
	for {
		select {
		case <-ticker.C:
			h.updateInterval()
			h.updateReservedDays()
			ticker.Reset(h.getCurInterval())
			if h.getCurReservedDays() == 0 {
				continue
			}
			if h.hotRegionStorageHelper.IsLeader() {
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
}

// NewIterator return a iterator which can traverse all data as request.
func (h *HotRegionStorage) NewIterator(requireTypes []string, startTime, endTime int64) HotRegionStorageIterator {
	iters := make([]iterator.Iterator, len(requireTypes))
	for index, requireType := range requireTypes {
		requireType = strings.ToLower(requireType)
		startKey := HotRegionStorePath(requireType, startTime, 0)
		endKey := HotRegionStorePath(requireType, endTime, math.MaxUint64)
		iter := h.LevelDBKV.NewIterator(&util.Range{Start: []byte(startKey), Limit: []byte(endKey)}, nil)
		iters[index] = iter
	}
	return HotRegionStorageIterator{
		iters:                iters,
		encryptionKeyManager: h.ekm,
	}
}

// Close closes the kv.
func (h *HotRegionStorage) Close() error {
	h.hotRegionInfoCancel()
	h.hotRegionLoopWg.Wait()
	if err := h.LevelDBKV.Close(); err != nil {
		return errs.ErrLevelDBClose.Wrap(err).GenWithStackByArgs()
	}
	return nil
}

func (h *HotRegionStorage) pullHotRegionInfo() error {
	historyHotReadRegions, err := h.hotRegionStorageHelper.GetHistoryHotRegions(utils.Read)
	if err != nil {
		return err
	}
	if err := h.packHistoryHotRegions(historyHotReadRegions, utils.Read.String()); err != nil {
		return err
	}
	historyHotWriteRegions, err := h.hotRegionStorageHelper.GetHistoryHotRegions(utils.Write)
	if err != nil {
		return err
	}
	return h.packHistoryHotRegions(historyHotWriteRegions, utils.Write.String())
}

func (h *HotRegionStorage) packHistoryHotRegions(historyHotRegions []HistoryHotRegion, hotRegionType string) error {
	for i := range historyHotRegions {
		region := &metapb.Region{
			Id:             historyHotRegions[i].RegionID,
			StartKey:       core.HexRegionKey([]byte(historyHotRegions[i].StartKey)),
			EndKey:         core.HexRegionKey([]byte(historyHotRegions[i].EndKey)),
			EncryptionMeta: historyHotRegions[i].EncryptionMeta,
		}
		region, err := encryption.EncryptRegion(region, h.ekm)
		if err != nil {
			return err
		}
		historyHotRegions[i].StartKey = string(region.StartKey)
		historyHotRegions[i].EndKey = string(region.EndKey)
		key := HotRegionStorePath(hotRegionType, historyHotRegions[i].UpdateTime, historyHotRegions[i].RegionID)
		h.batchHotInfo[key] = &historyHotRegions[i]
	}
	return nil
}

func (h *HotRegionStorage) updateInterval() {
	h.mu.Lock()
	defer h.mu.Unlock()
	interval := h.hotRegionStorageHelper.GetHotRegionsWriteInterval()
	if interval != h.curInterval {
		log.Info("hot region write interval changed",
			zap.Duration("previous-interval", h.curInterval),
			zap.Duration("new-interval", interval))
		h.curInterval = interval
	}
}

func (h *HotRegionStorage) getCurInterval() time.Duration {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.curInterval
}

func (h *HotRegionStorage) updateReservedDays() {
	h.mu.Lock()
	defer h.mu.Unlock()
	reservedDays := h.hotRegionStorageHelper.GetHotRegionsReservedDays()
	if reservedDays != h.curReservedDays {
		log.Info("hot region reserved days changed",
			zap.Uint64("previous-reserved-days", h.curReservedDays),
			zap.Uint64("new-reserved-days", reservedDays))
		h.curReservedDays = reservedDays
	}
}

func (h *HotRegionStorage) getCurReservedDays() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.curReservedDays
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
	if err := h.LevelDBKV.Write(batch, nil); err != nil {
		return errs.ErrLevelDBWrite.Wrap(err).GenWithStackByCause()
	}
	h.batchHotInfo = make(map[string]*HistoryHotRegion)
	return nil
}

func (h *HotRegionStorage) delete(reservedDays int) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	db := h.LevelDBKV
	batch := new(leveldb.Batch)
	for _, hotRegionType := range HotRegionTypes {
		startKey := HotRegionStorePath(hotRegionType, 0, 0)
		endTime := time.Now().AddDate(0, 0, 0-reservedDays).UnixNano() / int64(time.Millisecond)
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
	return nil
}

// HotRegionStorageIterator iterates over a historyHotRegion.
type HotRegionStorageIterator struct {
	iters                []iterator.Iterator
	encryptionKeyManager *encryption.Manager
}

// Next moves the iterator to the next key/value pair.
// And return historyHotRegion which it is now pointing to.
// it will return (nil, nil), if there is no more historyHotRegion.
func (it *HotRegionStorageIterator) Next() (*HistoryHotRegion, error) {
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
	var message HistoryHotRegion
	err := json.Unmarshal(value, &message)
	if err != nil {
		return nil, err
	}
	region := &metapb.Region{
		Id:             message.RegionID,
		StartKey:       []byte(message.StartKey),
		EndKey:         []byte(message.EndKey),
		EncryptionMeta: message.EncryptionMeta,
	}
	if err := encryption.DecryptRegion(region, it.encryptionKeyManager); err != nil {
		return nil, err
	}
	message.StartKey = string(region.StartKey)
	message.EndKey = string(region.EndKey)
	message.EncryptionMeta = nil
	return &message, nil
}

// HotRegionStorePath generate hot region store key for HotRegionStorage.
func HotRegionStorePath(hotRegionType string, updateTime int64, regionID uint64) string {
	return path.Join(
		"schedule",
		"hot_region",
		hotRegionType,
		fmt.Sprintf("%020d", updateTime),
		fmt.Sprintf("%020d", regionID),
	)
}
