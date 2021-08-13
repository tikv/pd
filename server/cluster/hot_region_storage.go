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
	defaultCompactionTime = 30
)

var hotRegionTypes = []string{
	"read",
	"write",
}

func NewHotRegionsHistoryStorage(
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
	h.backgroundFlush()
	h.backgroundDelete()
	return &h, nil
}

//delete hot_region info which update_time is smaller than time.Now() minus /remain day in the backgroud
func (h *HotRegionStorage) backgroundDelete() {
	//make delete happend in 0 clock
	now := time.Now()
	next := now.Add(time.Hour * 24)
	next = time.Date(next.Year(), next.Month(), next.Day(), 0, 0, 0, 0, next.Location())
	t := time.NewTicker(next.Sub(now))
	go func() {
		select {
		case <-t.C:

		case <-h.hotRegionInfoCtx.Done():
			return
		}
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
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

//write hot_region info into db in the backgroud
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

//return a iterator which can traverse from start_time to end_time
func (h *HotRegionStorage) NewIterator(requireTypes []string, startTime, endTime int64) HotRegionStorageIterator {
	iters := make([]iterator.Iterator, len(requireTypes))
	for index, requireType := range requireTypes {
		startKey := HotRegionStorePath(requireType, startTime, 0)
		endKey := HotRegionStorePath(requireType, endTime, math.MaxInt64)
		iter := h.LeveldbKV.NewIterator(&util.Range{[]byte(startKey), []byte(endKey)}, nil)
		iters[index] = iter
	}
	return HotRegionStorageIterator{
		iters:                iters,
		encryptionKeyManager: h.encryptionKeyManager,
	}
}

func (h *HotRegionStorage) Close() error {
	h.hotRegionInfoCancel()
	if err := h.LeveldbKV.Close(); err != nil {
		return errs.ErrLevelDBClose.Wrap(err).GenWithStackByArgs()
	}
	return nil
}

func (h *HotRegionStorage) pullHotRegionInfo() error {
	cluster := h.cluster
	hotReadLeaderInfo := cluster.coordinator.getHotReadRegions().AsLeader
	if err := h.packHotRegionInfo(hotReadLeaderInfo,
		"read"); err != nil {
		return err
	}
	hotWriteLeaderInfo := cluster.coordinator.getHotWriteRegions().AsLeader
	if err := h.packHotRegionInfo(hotWriteLeaderInfo,
		"write"); err != nil {
		return err
	}
	return nil
}

func (h *HotRegionStorage) packHotRegionInfo(hotLeaderInfo statistics.StoreHotPeersStat,
	hotRegionType string) error {
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
				UpdateTime:     hotPeerStat.LastUpdateTime.Unix(),
				RegionID:       hotPeerStat.RegionID,
				StoreID:        hotPeerStat.StoreID,
				PeerID:         peerID,
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
				hotPeerStat.LastUpdateTime.Unix(),
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
	for _, hotRegionType := range hotRegionTypes {
		startKey := HotRegionStorePath(hotRegionType, 0, 0)
		endTime := time.Now().AddDate(0, 0, 0-int(h.remianedDays)).Unix()
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
		for _, hotRegionType := range hotRegionTypes {
			startKey := HotRegionStorePath(hotRegionType, 0, 0)
			endTime := time.Now().AddDate(0, 0, 0-int(h.remianedDays)).Unix()
			endKey := HotRegionStorePath(hotRegionType, endTime, math.MaxInt64)
			db.CompactRange(util.Range{[]byte(startKey), []byte(endKey)})
		}
	}
	return nil
}

type HotRegionStorageIterator struct {
	iters                []iterator.Iterator
	encryptionKeyManager *encryptionkm.KeyManager
}

//next will return next history_hot_region,
//there is no more historyhotregion,it will return nil
func (it *HotRegionStorageIterator) Next() (*statistics.HistoryHotRegion, error) {
	iter := it.iters[0]
	if !iter.Next() {
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

//TODO
//find a better place to put this function
func HotRegionStorePath(hotRegionType string, update_time int64, region_id uint64) string {
	return path.Join(
		"schedule",
		"hot_region",
		hotRegionType,
		fmt.Sprintf("%020d", update_time),
		fmt.Sprintf("%020d", region_id),
	)
}
