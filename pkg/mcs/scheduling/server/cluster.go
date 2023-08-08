package server

import (
	"context"
	"time"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

// Cluster is used to manage all information for scheduling purpose.
type Cluster struct {
	*core.BasicCluster
	ruleManager    *placement.RuleManager
	labelerManager *labeler.RegionLabeler
	persistConfig  *config.PersistConfig
	hotStat        *statistics.HotStat
}

const regionLabelGCInterval = time.Hour

// NewCluster creates a new cluster.
func NewCluster(ctx context.Context, storage endpoint.RuleStorage, cfg *config.Config) (*Cluster, error) {
	basicCluster := core.NewBasicCluster()
	persistConfig := config.NewPersistConfig(cfg)
	labelerManager, err := labeler.NewRegionLabeler(ctx, storage, regionLabelGCInterval)
	if err != nil {
		return nil, err
	}

	return &Cluster{
		BasicCluster:   basicCluster,
		ruleManager:    placement.NewRuleManager(storage, basicCluster, persistConfig),
		labelerManager: labelerManager,
		persistConfig:  persistConfig,
		hotStat:        statistics.NewHotStat(ctx),
	}, nil
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

// TODO: implement the following methods

// GetStorage returns the storage.
func (c *Cluster) GetStorage() storage.Storage { return nil }

// UpdateRegionsLabelLevelStats updates the region label level stats.
func (c *Cluster) UpdateRegionsLabelLevelStats(regions []*core.RegionInfo) {}

// GetStoreConfig returns the store config.
func (c *Cluster) GetStoreConfig() sc.StoreConfigProvider { return nil }

// AllocID allocates a new ID.
func (c *Cluster) AllocID() (uint64, error) { return 0, nil }

// GetCheckerConfig returns the checker config.
func (c *Cluster) GetCheckerConfig() sc.CheckerConfigProvider { return nil }

// GetSchedulerConfig returns the scheduler config.
func (c *Cluster) GetSchedulerConfig() sc.SchedulerConfigProvider { return nil }
