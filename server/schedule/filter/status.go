// Copyright 2022 TiKV Project Authors.
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

package filter

import "github.com/tikv/pd/server/schedule/plan"

var (
	statusOK     = plan.NewStatus(plan.StatusOK)
	statusNoNeed = plan.NewStatus(plan.StatusNoNeed)

	// store filter status
	statusStoreDown               = plan.NewStatus(plan.StatusStoreUnavailable, "store is lost for longer than 'max-store-down-time' setting")
	statusStoreTombstone          = plan.NewStatus(plan.StatusStoreUnavailable, "store is tombstone")
	statusStoreDisconnected       = plan.NewStatus(plan.StatusStoreUnavailable, "store is lost for more than 20s")
	statusStoreBusy               = plan.NewStatus(plan.StatusStoreUnavailable, "store is busy")
	statusStoresOffline           = plan.NewStatus(plan.StatusStoreDraining, "store is in the process of offline")
	statusStoreLowSpace           = plan.NewStatus(plan.StatusStoreLowSpace, "store space is not enough, please scale out or change 'low-space-ratio' setting")
	statusStoreExcluded           = plan.NewStatus(plan.StatusStoreExcluded, "there has already had a peer or the peer is unhealthy in the store")
	statusStoreIsolation          = plan.NewStatus(plan.StatusIsolationNotMatch)
	statusStoreTooManySnapshot    = plan.NewStatus(plan.StatusStoreThrottled, "store snapshot has been piled up, the related setting is 'max-snapshot-count'")
	statusStoreTooManyPendingPeer = plan.NewStatus(plan.StatusStoreThrottled, "store has too many pending peers, the related setting is 'max-pending-peer-count'")
	statusStoreAddLimit           = plan.NewStatus(plan.StatusStoreThrottled, "store's add limit is exhausted, please check the setting of 'store limit'")
	statusStoreRemoveLimit        = plan.NewStatus(plan.StatusStoreThrottled, "store's remove limit is exhausted, please check the setting of 'store limit'")
	statusStoreSnapRemoveLimit    = plan.NewStatus(plan.StatusStoreThrottled, "store's snapshot remove limit is exhausted'")
	statusStoreLabel              = plan.NewStatus(plan.StatusLabelNotMatch)
	statusStoreRule               = plan.NewStatus(plan.StatusRuleNotMatch)
	statusStorePauseLeader        = plan.NewStatus(plan.StatusStoreBlocked, "the store is not allowed to transfer leader, there might be an evict-leader-scheduler")
	statusStoreRejectLeader       = plan.NewStatus(plan.StatusStoreBlocked, "the store is not allowed to transfer leader, please check 'label-property'")
	statusStoreSlow               = plan.NewStatus(plan.StatusStoreBlocked, "the store is slow and are evicting leaders, there might be an evict-slow-store-scheduler")

	// region filter status
	statusRegionPendingPeer   = plan.NewStatus(plan.StatusRegionUnhealthy, "region has pending peers")
	statusRegionDownPeer      = plan.NewStatus(plan.StatusRegionUnhealthy, "region has down peers")
	statusRegionEmpty         = plan.NewStatus(plan.StatusRegionEmpty)
	statusRegionRule          = plan.NewStatus(plan.StatusRuleNotMatch)
	statusRegionNotReplicated = plan.NewStatus(plan.StatusRegionNotReplicated)
)
