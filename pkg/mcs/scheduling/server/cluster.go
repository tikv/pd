package server

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/schedule"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
)

// Cluster is used to manage all information for scheduling purpose.
type Cluster struct {
	ctx context.Context
	*core.BasicCluster
	persistConfig     *config.PersistConfig
	ruleManager       *placement.RuleManager
	labelerManager    *labeler.RegionLabeler
	regionStats       *statistics.RegionStatistics
	hotStat           *statistics.HotStat
	storage           storage.Storage
	coordinator       *schedule.Coordinator
	checkMembershipCh chan struct{}
	apiServerLeader   atomic.Value
	clusterID         uint64
}

const regionLabelGCInterval = time.Hour

// NewCluster creates a new cluster.
func NewCluster(ctx context.Context, persistConfig *config.PersistConfig, storage storage.Storage, basicCluster *core.BasicCluster, hbStreams *hbstream.HeartbeatStreams, clusterID uint64, checkMembershipCh chan struct{}) (*Cluster, error) {
	labelerManager, err := labeler.NewRegionLabeler(ctx, storage, regionLabelGCInterval)
	if err != nil {
		return nil, err
	}
	ruleManager := placement.NewRuleManager(storage, basicCluster, persistConfig)
	c := &Cluster{
		ctx:               ctx,
		BasicCluster:      basicCluster,
		ruleManager:       ruleManager,
		labelerManager:    labelerManager,
		persistConfig:     persistConfig,
		hotStat:           statistics.NewHotStat(ctx),
		regionStats:       statistics.NewRegionStatistics(basicCluster, persistConfig, ruleManager),
		storage:           storage,
		clusterID:         clusterID,
		checkMembershipCh: checkMembershipCh,
	}
	c.coordinator = schedule.NewCoordinator(ctx, c, hbStreams)
	err = c.ruleManager.Initialize(persistConfig.GetMaxReplicas(), persistConfig.GetLocationLabels())
	if err != nil {
		return nil, err
	}
	return c, nil
}

// GetCoordinator returns the coordinator
func (c *Cluster) GetCoordinator() *schedule.Coordinator {
	return c.coordinator
}

// GetBasicCluster returns the basic cluster.
func (c *Cluster) GetBasicCluster() *core.BasicCluster {
	return c.BasicCluster
}

// GetSharedConfig returns the shared config.
func (c *Cluster) GetSharedConfig() sc.SharedConfigProvider {
	return c.persistConfig
}

// GetRuleManager returns the rule manager.
func (c *Cluster) GetRuleManager() *placement.RuleManager {
	return c.ruleManager
}

// GetRegionLabeler returns the region labeler.
func (c *Cluster) GetRegionLabeler() *labeler.RegionLabeler {
	return c.labelerManager
}

// GetStoresLoads returns load stats of all stores.
func (c *Cluster) GetStoresLoads() map[uint64][]float64 {
	return c.hotStat.GetStoresLoads()
}

// IsRegionHot checks if a region is in hot state.
func (c *Cluster) IsRegionHot(region *core.RegionInfo) bool {
	return c.hotStat.IsRegionHot(region, c.persistConfig.GetHotRegionCacheHitsThreshold())
}

// GetHotPeerStat returns hot peer stat with specified regionID and storeID.
func (c *Cluster) GetHotPeerStat(rw utils.RWType, regionID, storeID uint64) *statistics.HotPeerStat {
	return c.hotStat.GetHotPeerStat(rw, regionID, storeID)
}

// RegionReadStats returns hot region's read stats.
// The result only includes peers that are hot enough.
// RegionStats is a thread-safe method
func (c *Cluster) RegionReadStats() map[uint64][]*statistics.HotPeerStat {
	// As read stats are reported by store heartbeat, the threshold needs to be adjusted.
	threshold := c.persistConfig.GetHotRegionCacheHitsThreshold() *
		(utils.RegionHeartBeatReportInterval / utils.StoreHeartBeatReportInterval)
	return c.hotStat.RegionStats(utils.Read, threshold)
}

// RegionWriteStats returns hot region's write stats.
// The result only includes peers that are hot enough.
func (c *Cluster) RegionWriteStats() map[uint64][]*statistics.HotPeerStat {
	// RegionStats is a thread-safe method
	return c.hotStat.RegionStats(utils.Write, c.persistConfig.GetHotRegionCacheHitsThreshold())
}

// BucketsStats returns hot region's buckets stats.
func (c *Cluster) BucketsStats(degree int, regionIDs ...uint64) map[uint64][]*buckets.BucketStat {
	return c.hotStat.BucketsStats(degree, regionIDs...)
}

// GetStorage returns the storage.
func (c *Cluster) GetStorage() storage.Storage {
	return c.storage
}

// GetCheckerConfig returns the checker config.
func (c *Cluster) GetCheckerConfig() sc.CheckerConfigProvider { return c.persistConfig }

// GetSchedulerConfig returns the scheduler config.
func (c *Cluster) GetSchedulerConfig() sc.SchedulerConfigProvider { return c.persistConfig }

// GetStoreConfig returns the store config.
func (c *Cluster) GetStoreConfig() sc.StoreConfigProvider { return c.persistConfig }

// AllocID allocates a new ID.
func (c *Cluster) AllocID() (uint64, error) {
	cli := c.apiServerLeader.Load().(pdpb.PDClient)
	if cli == nil {
		c.checkMembershipCh <- struct{}{}
		return 0, errors.New("API server leader is not found")
	}
	resp, err := cli.AllocID(c.ctx, &pdpb.AllocIDRequest{Header: &pdpb.RequestHeader{ClusterId: c.clusterID}})
	if err != nil {
		c.checkMembershipCh <- struct{}{}
		return 0, err
	}
	return resp.GetId(), nil
}

// SwitchAPIServerLeader switches the API server leader.
func (c *Cluster) SwitchAPIServerLeader(new pdpb.PDClient) bool {
	old := c.apiServerLeader.Load()
	return c.apiServerLeader.CompareAndSwap(old, new)
}

// TODO: implement the following methods

// UpdateRegionsLabelLevelStats updates the status of the region label level by types.
func (c *Cluster) UpdateRegionsLabelLevelStats(regions []*core.RegionInfo) {
}
