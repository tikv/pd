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

package core

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
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/encryptionkm"
	"github.com/tikv/pd/server/kv"
)

// HotRegionStorage is used to storage hot region info,
// It will pull the hot region information according to the pullInterval interval.
// And delete and save data beyond the remainingDays.
// Close must be called after use.
type HotRegionStorage struct {
	*kv.LeveldbKV
	encryptionKeyManager    *encryptionkm.KeyManager
	mu                      sync.RWMutex
	hotRegionLoopWg         sync.WaitGroup
	batchHotInfo            map[string]*HistoryHotRegion
	remianedDays            int64
	pullInterval            time.Duration
	hotRegionInfoCtx        context.Context
	hotRegionInfoCancel     context.CancelFunc
	hotRegionStorageHandler HotRegionStorageHandler
}

// HistoryHotRegions wraps historyHotRegion
// it will return to tidb
type HistoryHotRegions struct {
	HistoryHotRegion []*HistoryHotRegion `json:"history_hot_region"`
}

// HistoryHotRegion wraps hot region info
// it is storage format of hot_region_storage
type HistoryHotRegion struct {
	UpdateTime    int64   `json:"update_time,omitempty"`
	RegionID      uint64  `json:"region_id,omitempty"`
	PeerID        uint64  `json:"peer_id,omitempty"`
	StoreID       uint64  `json:"store_id,omitempty"`
	IsLeader      bool    `json:"is_leader,omitempty"`
	IsLearner     bool    `json:"is_learner,omitempty"`
	HotRegionType string  `json:"hot_region_type,omitempty"`
	HotDegree     int64   `json:"hot_degree,omitempty"`
	FlowBytes     float64 `json:"flow_bytes,omitempty"`
	KeyRate       float64 `json:"key_rate,omitempty"`
	QueryRate     float64 `json:"query_rate,omitempty"`
	StartKey      []byte  `json:"start_key,omitempty"`
	EndKey        []byte  `json:"end_key,omitempty"`
	// Encryption metadata for start_key and end_key. encryption_meta.iv is IV for start_key.
	// IV for end_key is calculated from (encryption_meta.iv + len(start_key)).
	// The field is only used by PD and should be ignored otherwise.
	// If encryption_meta is empty (i.e. nil), it means start_key and end_key are unencrypted.
	EncryptionMeta *encryptionpb.EncryptionMeta `json:"encryption_meta,omitempty"`
}

// HotRegionStorageHandler help hot region storage get hot region info.
type HotRegionStorageHandler interface {
	// PackHistoryHotWriteRegions get read hot region info in HistoryHotRegion form.
	PackHistoryHotReadRegions() ([]HistoryHotRegion, error)
	// PackHistoryHotWriteRegions get write hot region info in HistoryHotRegion form.
	PackHistoryHotWriteRegions() ([]HistoryHotRegion, error)
	// IsLeader return true means this server is leader.
	IsLeader() bool
}

const (
	// delete will run at this o`clock.
	defaultDeleteTime = 4
)

// HotRegionType stands for hot type.
type HotRegionType uint32

// Flags for flow.
const (
	WriteType HotRegionType = iota
	ReadType
)

// HotRegionTypes stands for hot type.
var HotRegionTypes = []string{
	WriteType.String(),
	ReadType.String(),
}

// String return HotRegionType in string format.
func (h HotRegionType) String() string {
	switch h {
	case WriteType:
		return "write"
	case ReadType:
		return "read"
	}
	return "unimplemented"
}

// NewHotRegionsStorage create storage to store hot regions info.
func NewHotRegionsStorage(
	ctx context.Context,
	path string,
	encryptionKeyManager *encryptionkm.KeyManager,
	hotRegionStorageHandler HotRegionStorageHandler,
	remianedDays int64,
	pullInterval time.Duration,
) (*HotRegionStorage, error) {
	levelDB, err := kv.NewLeveldbKV(path)
	if err != nil {
		return nil, err
	}
	hotRegionInfoCtx, hotRegionInfoCancle := context.WithCancel(ctx)
	h := HotRegionStorage{
		LeveldbKV:               levelDB,
		encryptionKeyManager:    encryptionKeyManager,
		batchHotInfo:            make(map[string]*HistoryHotRegion),
		remianedDays:            remianedDays,
		pullInterval:            pullInterval,
		hotRegionInfoCtx:        hotRegionInfoCtx,
		hotRegionInfoCancel:     hotRegionInfoCancle,
		hotRegionStorageHandler: hotRegionStorageHandler,
	}
	if remianedDays > 0 {
		h.hotRegionLoopWg.Add(2)
		go h.backgroundFlush()
		go h.backgroundDelete()
	}
	return &h, nil
}

// delete hot region which update_time is smaller than time.Now() minus remain day in the background.
func (h *HotRegionStorage) backgroundDelete() {
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
			if isFirst {
				ticker.Reset(24 * time.Hour)
				isFirst = false
			}
			h.delete()
		case <-h.hotRegionInfoCtx.Done():
			return
		}
	}
}

// Write hot_region info into db in the background.
func (h *HotRegionStorage) backgroundFlush() {
	ticker := time.NewTicker(h.pullInterval)
	defer func() {
		ticker.Stop()
		h.hotRegionLoopWg.Done()
	}()
	for {
		select {
		case <-ticker.C:
			if h.hotRegionStorageHandler.IsLeader() {
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

// NewIterator return a iterator which can traverse all data as reqeust.
func (h *HotRegionStorage) NewIterator(requireTypes []string, startTime, endTime int64) HotRegionStorageIterator {
	iters := make([]iterator.Iterator, len(requireTypes))
	for index, requireType := range requireTypes {
		requireType = strings.ToLower(requireType)
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
	h.hotRegionLoopWg.Wait()
	if err := h.LeveldbKV.Close(); err != nil {
		return errs.ErrLevelDBClose.Wrap(err).GenWithStackByArgs()
	}
	return nil
}

func (h *HotRegionStorage) pullHotRegionInfo() error {
	historyHotReadRegions, err := h.hotRegionStorageHandler.PackHistoryHotReadRegions()
	if err != nil {
		return err
	}
	if err := h.packHistoryHotRegions(historyHotReadRegions, ReadType.String()); err != nil {
		return err
	}
	historyHotWriteRegions, err := h.hotRegionStorageHandler.PackHistoryHotWriteRegions()
	if err != nil {
		return err
	}
	err = h.packHistoryHotRegions(historyHotWriteRegions, WriteType.String())
	return err
}

func (h *HotRegionStorage) packHistoryHotRegions(historyHotRegions []HistoryHotRegion, hotRegionType string) error {
	for i := range historyHotRegions {
		region := &metapb.Region{
			Id:             historyHotRegions[i].RegionID,
			StartKey:       historyHotRegions[i].StartKey,
			EndKey:         historyHotRegions[i].EndKey,
			EncryptionMeta: historyHotRegions[i].EncryptionMeta,
		}
		region, err := encryption.EncryptRegion(region, h.encryptionKeyManager)
		if err != nil {
			return err
		}
		historyHotRegions[i].StartKey = region.StartKey
		historyHotRegions[i].EndKey = region.EndKey
		key := HotRegionStorePath(hotRegionType, historyHotRegions[i].UpdateTime, historyHotRegions[i].RegionID)
		h.batchHotInfo[key] = &historyHotRegions[i]
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
	h.batchHotInfo = make(map[string]*HistoryHotRegion)
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
	return nil
}

// HotRegionStorageIterator iterates over a historyHotRegion.
type HotRegionStorageIterator struct {
	iters                []iterator.Iterator
	encryptionKeyManager *encryptionkm.KeyManager
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
