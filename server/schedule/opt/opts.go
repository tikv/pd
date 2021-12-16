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

package opt

import (
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/server/versioninfo"
)

// Cluster provides an overview of a cluster's regions distribution.
// TODO: This interface should be moved to a better place.
type Cluster interface {
	core.RegionSetInformer
	core.StoreSetInformer
	core.StoreSetController

	statistics.RegionStatInformer
	statistics.StoreStatInformer

	GetOpts() *config.PersistOptions
	AllocID() (uint64, error)
	GetRuleManager() *placement.RuleManager
	RemoveScheduler(name string) error
	IsFeatureSupported(f versioninfo.Feature) bool
	AddSuspectRegions(ids ...uint64)
	GetBasicCluster() *core.BasicCluster
}

// HeartbeatStream is an interface.
type HeartbeatStream interface {
	Send(*pdpb.RegionHeartbeatResponse) error
}
