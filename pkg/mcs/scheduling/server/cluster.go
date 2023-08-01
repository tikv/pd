package server

import (
	"context"
	"time"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

// Cluster is used to manage all information for scheduling purpose.
type Cluster struct {
	basicCluster   *core.BasicCluster
	ruleManager    *placement.RuleManager
	labelerManager *labeler.RegionLabeler
	persistConfig  *config.PersistConfig
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
		basicCluster:   basicCluster,
		ruleManager:    placement.NewRuleManager(storage, basicCluster, persistConfig),
		labelerManager: labelerManager,
		persistConfig:  persistConfig,
	}, nil
}
