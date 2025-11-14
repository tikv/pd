// Copyright 2017 TiKV Project Authors.
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

package core

import (
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/keyrange"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/storage"
)

// ClusterInformer provides the necessary information of a cluster.
type ClusterInformer interface {
	SchedulerCluster
	CheckerCluster

	GetStorage() storage.Storage
}

// SchedulerCluster is an aggregate interface that wraps multiple interfaces
type SchedulerCluster interface {
	SharedCluster

	statistics.StoreStatInformer
	buckets.BucketStatInformer

	GetSchedulerConfig() sc.SchedulerConfigProvider
	GetRegionLabeler() *labeler.RegionLabeler
	GetStoreConfig() sc.StoreConfigProvider
}

// CheckerCluster is an aggregate interface that wraps multiple interfaces
type CheckerCluster interface {
	SharedCluster

	GetCheckerConfig() sc.CheckerConfigProvider
	GetStoreConfig() sc.StoreConfigProvider
	UpdateRegionsLabelLevelStats(regions []*core.RegionInfo)
}

// SharedCluster is an aggregate interface that wraps multiple interfaces
type SharedCluster interface {
	BasicCluster
	statistics.RegionStatInformer

	GetBasicCluster() *core.BasicCluster
	GetSharedConfig() sc.SharedConfigProvider
	GetRuleManager() *placement.RuleManager
	GetAffinityManager() *affinity.Manager
	GetKeyRangeManager() *keyrange.Manager
	AllocID(uint32) (uint64, uint32, error)
	IsSchedulingHalted() bool
	GetPrepareRegionCount() (int, error)
}

// BasicCluster is an aggregate interface that wraps multiple interfaces
type BasicCluster interface {
	core.StoreSetInformer
	core.StoreSetController
	core.RegionSetInformer
}
