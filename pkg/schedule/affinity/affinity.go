// Copyright 2025 TiKV Project Authors.
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

package affinity

import (
	"context"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/labeler"
)

// Group defines an affinity group. Regions belonging to it will tend to have the same distribution.
type Group struct {
	// ID is a unique identifier for Group.
	ID string
	// CreateTimestamp is the time when the Group was created.
	CreateTimestamp uint64

	// The following parameters are all determined automatically.

	// LeaderStoreID indicates which store the leader should be on.
	LeaderStoreID uint64
	// VoterStoreIDs indicates which stores Voters should be on.
	VoterStoreIDs []uint64
	// TODO: LearnerStoreIDs
}

// GroupInfo contains meta information and runtime statistics for the Group.
type GroupInfo struct {
	Group

	// Effect parameter indicates whether the current constraint is in effect.
	// Constraints are typically released when the store is in an abnormal state.
	Effect bool
	// AffinityRegionCount indicates how many Regions are currently in the affinity state.
	AffinityRegionCount uint64

	regions map[uint64]struct{}
	labels  map[string]*labeler.LabelRule
}

// Manager is the manager of all affinity information.
type Manager struct {
	ctx context.Context
	// TODO: storage endpoint.AffinityStorage
	// TODO: maybe RWMutex
	initialized      bool
	groups           map[string]*GroupInfo
	regions          map[uint64]*GroupInfo
	storeSetInformer core.StoreSetInformer
	conf             config.SharedConfigProvider
}
