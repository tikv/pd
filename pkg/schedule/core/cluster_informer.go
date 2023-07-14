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
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/server/config"
)

// ClusterInformer provides the necessary information of a cluster.
type ClusterInformer interface {
	ScheduleCluster

	GetStorage() storage.Storage
	UpdateRegionsLabelLevelStats(regions []*core.RegionInfo)
	AddSuspectRegions(ids ...uint64)
	GetPersistOptions() *config.PersistOptions
}

// ScheduleCluster is an aggregate interface that wraps multiple interfaces for schedulers use
type ScheduleCluster interface {
	BasicCluster

	statistics.StoreStatInformer
	statistics.RegionStatInformer
	buckets.BucketStatInformer

	GetOpts() sc.Config
	GetRuleManager() *placement.RuleManager
	GetRegionLabeler() *labeler.RegionLabeler
	GetBasicCluster() *core.BasicCluster
	GetStoreConfig() sc.StoreConfig
	AllocID() (uint64, error)
}

// BasicCluster is an aggregate interface that wraps multiple interfaces
type BasicCluster interface {
	core.StoreSetInformer
	core.StoreSetController
	core.RegionSetInformer
}
