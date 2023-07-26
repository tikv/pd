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
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/statistics"
	"go.uber.org/zap"
)

const (
	// ShuffleHotRegionName is shuffle hot region scheduler name.
	ShuffleHotRegionName = "shuffle-hot-region-scheduler"
	// ShuffleHotRegionType is shuffle hot region scheduler type.
	ShuffleHotRegionType = "shuffle-hot-region"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(ShuffleHotRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*shuffleHotRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			conf.Limit = uint64(1)
			if len(args) == 1 {
				limit, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
				}
				conf.Limit = limit
			}
			conf.Name = ShuffleHotRegionName
			return nil
		}
	})

	schedule.RegisterScheduler(ShuffleHotRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleHotRegionSchedulerConfig{Limit: uint64(1)}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleHotRegionScheduler(opController, conf), nil
	})
}

type shuffleHotRegionSchedulerConfig struct {
	Name  string `json:"name"`
	Limit uint64 `json:"limit"`
}

type modeTypeShuffle int

const (
	twoDimRelaxModeShuffle = iota
	randomModeShuffle
	bytesDimRelaxModeShuffle
	keysDimRelaxModeShuffle
	twoDimStrictModeShuffle
	bytesDimStrictModeShuffle
	keysDimStrictModeShuffle
)

// ShuffleHotRegionScheduler mainly used to test.
// It will randomly pick a hot peer, and move the peer
// to a random store, and then transfer the leader to
// the hot peer.
type shuffleHotRegionScheduler struct {
	*BaseScheduler
	stLoadInfos [resourceTypeLen]map[uint64]*storeLoadDetail
	r           *rand.Rand
	conf        *shuffleHotRegionSchedulerConfig
	types       []rwType

	mode         modeTypeShuffle
	schedRegions map[uint64][]uint64
}

// newShuffleHotRegionScheduler creates an admin scheduler that random balance hot regions
func newShuffleHotRegionScheduler(opController *schedule.OperatorController, conf *shuffleHotRegionSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	ret := &shuffleHotRegionScheduler{
		BaseScheduler: base,
		conf:          conf,
		types:         []rwType{read, write},
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
		schedRegions:  make(map[uint64][]uint64),
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.stLoadInfos[ty] = map[uint64]*storeLoadDetail{}
	}
	return ret
}

func (s *shuffleHotRegionScheduler) GetName() string {
	return s.conf.Name
}

func (s *shuffleHotRegionScheduler) GetType() string {
	return ShuffleHotRegionType
}

func (s *shuffleHotRegionScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *shuffleHotRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.OpController.OperatorCount(operator.OpHotRegion) < s.conf.Limit &&
		s.OpController.OperatorCount(operator.OpRegion) < cluster.GetOpts().GetRegionScheduleLimit() &&
		s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
}

func (s *shuffleHotRegionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	// i := s.r.Int() % len(s.types)
	//jk: change for read dim     ref:
	//types:         []rwType{read, write},
	//return s.dispatchStatic(s.types[0], cluster)
	return s.dispatchStatic(s.types[1], cluster)
	// return s.dispatch(s.types[i], cluster)
}

func (s *shuffleHotRegionScheduler) dispatch(typ rwType, cluster opt.Cluster) []*operator.Operator {
	storesStats := cluster.GetStoresStats()
	minHotDegree := cluster.GetOpts().GetHotRegionCacheHitsThreshold()
	switch typ {
	case read:
		hotRegionThreshold := getHotRegionThreshold(storesStats, read)
		s.stLoadInfos[readLeader] = summaryStoresLoad(
			storesStats.GetStoresBytesReadStat(),
			storesStats.GetStoresKeysReadStat(),
			map[uint64]Influence{},
			cluster.RegionReadStats(),
			minHotDegree,
			hotRegionThreshold,
			read, core.LeaderKind, mixed)
		return s.randomSchedule(cluster, s.stLoadInfos[readLeader])
	case write:
		hotRegionThreshold := getHotRegionThreshold(storesStats, write)
		s.stLoadInfos[writeLeader] = summaryStoresLoad(
			storesStats.GetStoresBytesWriteStat(),
			storesStats.GetStoresKeysWriteStat(),
			map[uint64]Influence{},
			cluster.RegionWriteStats(),
			minHotDegree,
			hotRegionThreshold,
			write, core.LeaderKind, mixed)
		return s.randomSchedule(cluster, s.stLoadInfos[writeLeader])
	}
	return nil
}

func (s *shuffleHotRegionScheduler) randomSchedule(cluster opt.Cluster, loadDetail map[uint64]*storeLoadDetail) []*operator.Operator {
	for _, detail := range loadDetail {
		if len(detail.HotPeers) < 1 {
			continue
		}
		i := s.r.Intn(len(detail.HotPeers))
		r := detail.HotPeers[i]
		// select src region
		srcRegion := cluster.GetRegion(r.RegionID)
		if srcRegion == nil || len(srcRegion.GetDownPeers()) != 0 || len(srcRegion.GetPendingPeers()) != 0 {
			continue
		}
		srcStoreID := srcRegion.GetLeader().GetStoreId()
		srcStore := cluster.GetStore(srcStoreID)
		if srcStore == nil {
			log.Error("failed to get the source store", zap.Uint64("store-id", srcStoreID), errs.ZapError(errs.ErrGetSourceStore))
		}

		filters := []filter.Filter{
			filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true},
			filter.NewExcludedFilter(s.GetName(), srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
			filter.NewPlacementSafeguard(s.GetName(), cluster, srcRegion, srcStore),
		}
		stores := cluster.GetStores()
		destStoreIDs := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if !filter.Target(cluster.GetOpts(), store, filters) {
				continue
			}
			destStoreIDs = append(destStoreIDs, store.GetID())
		}
		if len(destStoreIDs) == 0 {
			return nil
		}
		// random pick a dest store
		destStoreID := destStoreIDs[s.r.Intn(len(destStoreIDs))]
		if destStoreID == 0 {
			return nil
		}
		srcPeer := srcRegion.GetStorePeer(srcStoreID)
		if srcPeer == nil {
			return nil
		}
		destPeer := &metapb.Peer{StoreId: destStoreID}
		op, err := operator.CreateMoveLeaderOperator("random-move-hot-leader", cluster, srcRegion, operator.OpRegion|operator.OpLeader, srcStoreID, destPeer)
		if err != nil {
			log.Debug("fail to create move leader operator", errs.ZapError(err))
			return nil
		}
		op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
		return []*operator.Operator{op}
	}
	schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
	log.Info("skip random shuffle")
	return nil
}

func (s *shuffleHotRegionScheduler) dispatchStatic(typ rwType, cluster opt.Cluster) []*operator.Operator {
	storesStats := cluster.GetStoresStats()
	minHotDegree := cluster.GetOpts().GetHotRegionCacheHitsThreshold()
	s.mode = modeTypeShuffle(cluster.GetOpts().GetShuffleHotSchedulerMode())
	switch typ {
	case read:
		hotRegionThreshold := getHotRegionThreshold(storesStats, read)
		if len(s.stLoadInfos[readLeader]) == 0 {
			s.stLoadInfos[readLeader] = summaryStoresLoad(
				storesStats.GetStoresBytesReadStat(),
				storesStats.GetStoresKeysReadStat(),
				map[uint64]Influence{},
				cluster.RegionReadStats(),
				minHotDegree,
				hotRegionThreshold,
				read, core.LeaderKind, mixed)
		}
		//jk: add two dim shuffle
		//return s.randomSchedule(cluster, s.stLoadInfos[readLeader])
		if s.mode == randomModeShuffle {
			return s.randomSchedule(cluster, s.stLoadInfos[readLeader])
		}
		return s.twoDimShuffle(cluster, s.stLoadInfos[readLeader])
	case write:
		hotRegionThreshold := getHotRegionThreshold(storesStats, write)
		if len(s.stLoadInfos[writePeer]) == 0 {
			s.stLoadInfos[writePeer] = summaryStoresLoad(
				storesStats.GetStoresBytesWriteStat(),
				storesStats.GetStoresKeysWriteStat(),
				map[uint64]Influence{},
				cluster.RegionWriteStats(),
				minHotDegree,
				hotRegionThreshold,
				write, core.RegionKind, mixed)
		}
		if s.mode == randomModeShuffle {
			return s.randomSchedule(cluster, s.stLoadInfos[writePeer])
		}
		return s.twoDimShuffle(cluster, s.stLoadInfos[writePeer])
	}
	return nil
}

type loadDetailPair struct {
	storeID uint64
	detail  *storeLoadDetail
}

func showBalanceRatio(loadDetail map[uint64]*storeLoadDetail) bool {
	loadMaps := make([]map[uint64]float64, 2)
	maxRatio := 0.0
	for dimID := 0; dimID < 2; dimID++ {
		loadMap := make(map[uint64]float64)
		loadMaps[dimID] = loadMap
		var maxLoad, avgLoad float64
		for storeID, detail := range loadDetail {
			load := 0.0
			if dimID == 0 {
				load = detail.LoadPred.Current.ByteRate
			} else {
				load = detail.LoadPred.Current.KeyRate
			}
			avgLoad += load
			maxLoad = math.Max(maxLoad, load)
			loadMap[storeID] = load
		}
		avgLoad /= float64(len(loadDetail))
		maxRatio = math.Max(maxRatio, maxLoad/avgLoad)

		for storeID := range loadMap {
			loadMap[storeID] /= avgLoad
		}
		log.Info("printLoad",
			zap.Int("dimID", dimID),
			zap.Float64("balanceRatio", maxLoad/avgLoad),
			zap.String("loadRatio", fmt.Sprintf("%+v", loadMap)),
		)
	}

	inter := true
	for id := range loadMaps[0] {
		if (loadMaps[0][id]-1.0)*(loadMaps[1][id]-1.0) >= 0.0 {
			inter = false
			break
		}
	}
	if inter && maxRatio > 1.5 {
		return true
	}
	return false
}

func (s *shuffleHotRegionScheduler) parseMode(cluster opt.Cluster, loadDetail map[uint64]*storeLoadDetail) (balanceType int, relaxedSelection bool) {
	balanceType = s.r.Int() % 2
	showBalanceRatio(loadDetail)

	switch s.mode {
	case twoDimRelaxModeShuffle:
		relaxedSelection = true
	case bytesDimRelaxModeShuffle:
		relaxedSelection = true
		balanceType = 0
	case keysDimRelaxModeShuffle:
		relaxedSelection = true
		balanceType = 1
	case twoDimStrictModeShuffle:
		relaxedSelection = false
	case bytesDimStrictModeShuffle:
		relaxedSelection = false
		balanceType = 0
	case keysDimStrictModeShuffle:
		relaxedSelection = false
		balanceType = 1
	}

	return
}

func (s *shuffleHotRegionScheduler) twoDimShuffle(cluster opt.Cluster, loadDetail map[uint64]*storeLoadDetail) []*operator.Operator {
	balanceType, relaxedSelection := s.parseMode(cluster, loadDetail)

	details := []*loadDetailPair{}
	for srcStoreID, detail := range loadDetail {
		details = append(details, &loadDetailPair{
			storeID: srcStoreID,
			detail:  detail,
		})
	}

	sort.Slice(details, func(i, j int) bool {
		if balanceType == 0 {
			return details[i].detail.LoadPred.Current.ByteRate < details[j].detail.LoadPred.Current.ByteRate
		}
		return details[i].detail.LoadPred.Current.ByteRate >= details[j].detail.LoadPred.Current.ByteRate
	})
	storeLen := len(loadDetail)
	// targetStoreCount := len(loadDetail) - len(loadDetail)/2

	for tryCount := 0; tryCount < 5; tryCount++ {
		storeIndex := s.r.Int() % (len(loadDetail))
		detailPair := details[storeIndex]
		detail := detailPair.detail
		if storeIndex == storeLen-1 {
			break
		}
		if len(detail.HotPeers) < 1 {
			continue
		}

		sort.Slice(detail.HotPeers, func(i, j int) bool {
			if balanceType == 0 {
				return detail.HotPeers[i].GetByteRate() > detail.HotPeers[j].GetByteRate()
			}
			return detail.HotPeers[i].GetKeyRate() > detail.HotPeers[j].GetKeyRate()
		})

		var peerIndex int
		var peer *statistics.HotPeerStat
		var found bool
		for peerIndex, peer = range detail.HotPeers {
			approxKVSize := peer.GetByteRate() / peer.GetKeyRate()
			bytePercent := peer.GetByteRate() / detail.LoadPred.Future.ExpByteRate
			keyPercent := peer.GetKeyRate() / detail.LoadPred.Future.ExpKeyRate
			if relaxedSelection {
				if balanceType == 0 && bytePercent > keyPercent {
					found = true
					break
				} else if balanceType == 1 && keyPercent > bytePercent {
					found = true
					break
				}
			} else {
				if balanceType == 0 && approxKVSize > 768 && bytePercent > keyPercent {
					if bytePercent > 0.01 {
						found = true
						break
					}
				} else if balanceType == 1 && approxKVSize < 400 && keyPercent > bytePercent {
					if keyPercent > 0.01 {
						found = true
						break
					}
				}
			}
		}

		if !found {
			continue
		}

		srcStoreID := detailPair.storeID
		// dstStoreIndex := s.r.Intn(targetStoreCount) + storeLen/2
		dstStoreIndex := s.r.Intn(storeLen-storeIndex-1) + storeIndex + 1
		dstStoreID := details[dstStoreIndex].storeID

		ops := s.check(cluster, peer, srcStoreID, dstStoreID)
		if ops == nil {
			continue
		}

		detail.HotPeers = append(detail.HotPeers[:peerIndex], detail.HotPeers[peerIndex+1:]...)
		detail.LoadPred.Current.ByteRate -= peer.GetByteRate()
		detail.LoadPred.Current.KeyRate -= peer.GetKeyRate()

		dstDetail := details[dstStoreIndex].detail
		dstDetail.HotPeers = append(dstDetail.HotPeers, peer)
		dstDetail.LoadPred.Current.ByteRate += peer.GetByteRate()
		dstDetail.LoadPred.Current.KeyRate += peer.GetKeyRate()

		log.Info("shuffle: move peer info",
			zap.Uint64("regionID", peer.RegionID),
			zap.Uint64("srcStore", srcStoreID),
			zap.Uint64("dstStore", dstStoreID),
			zap.Float64("byteRatio", peer.GetByteRate()/detail.LoadPred.Future.ExpByteRate),
			zap.Float64("keyRatio", peer.GetKeyRate()/detail.LoadPred.Future.ExpKeyRate),
			zap.Float64("approaxKVSize", peer.GetByteRate()/peer.GetKeyRate()),
		)

		return ops
	}
	schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
	return nil
}

func (s *shuffleHotRegionScheduler) check(cluster opt.Cluster, peer *statistics.HotPeerStat, srcStoreID, dstStoreID uint64) []*operator.Operator {
	// select src region
	srcRegion := cluster.GetRegion(peer.RegionID)
	if srcRegion == nil || len(srcRegion.GetDownPeers()) != 0 || len(srcRegion.GetPendingPeers()) != 0 {
		return nil
	}
	srcStore := cluster.GetStore(srcStoreID)
	if srcStore == nil {
		log.Error("failed to get the source store", zap.Uint64("store-id", srcStoreID), errs.ZapError(errs.ErrGetSourceStore))
		return nil
	}
	srcPeer := srcRegion.GetStorePeer(srcStoreID)
	if srcPeer == nil {
		return nil
	}

	{
		dstPeer := srcRegion.GetStorePeer(dstStoreID)
		if dstPeer != nil {
			return nil
		}
	}

	destPeer := &metapb.Peer{StoreId: dstStoreID}

	var op *operator.Operator
	var err error
	if srcRegion.GetLeader().GetStoreId() == srcStoreID {
		op, err = operator.CreateMoveLeaderOperator("random-move-hot-leader", cluster, srcRegion, operator.OpHotRegion, srcStoreID, destPeer)
	} else {
		op, err = operator.CreateMovePeerOperator("random-move-hot-leader", cluster, srcRegion, operator.OpHotRegion, srcStoreID, destPeer)
	}

	if err != nil {
		log.Debug("fail to create move leader operator", errs.ZapError(err))
		return nil
	}
	op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))

	return []*operator.Operator{op}
}
