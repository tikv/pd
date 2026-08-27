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

import "github.com/tikv/pd/pkg/schedule/plan"

var (
	statusOK = plan.NewStatus(plan.StatusOK)

	statusStoreScoreDisallowed = plan.NewStatus(plan.StatusStoreScoreDisallowed)
	statusStoreAlreadyHasPeer  = plan.NewStatus(plan.StatusStoreAlreadyHasPeer)

	// store hard limitation
	statusStoreDown         = plan.NewStatus(plan.StatusStoreDown)
	statusStoreRemoved      = plan.NewStatus(plan.StatusStoreRemoved)
	statusStoreDisconnected = plan.NewStatus(plan.StatusStoreDisconnected)
	statusStoresRemoving    = plan.NewStatus(plan.StatusStoreRemoving)
	statusStoreLowSpace     = plan.NewStatus(plan.StatusStoreLowSpace)
	statusStoreBusy         = plan.NewStatus(plan.StatusStoreBusy)

	// store soft limitation
	statusStoreSnapshotThrottled    = plan.NewStatus(plan.StatusStoreSnapshotThrottled)
	statusStorePendingPeerThrottled = plan.NewStatus(plan.StatusStorePendingPeerThrottled)
	statusStoreAddLimit             = plan.NewStatus(plan.StatusStoreAddLimitThrottled)
	statusStoreRemoveLimit          = plan.NewStatus(plan.StatusStoreRemoveLimitThrottled)

	// store config limitation
	statusStoreRejectLeader = plan.NewStatus(plan.StatusStoreRejectLeader)

	statusStoreNotMatchRule      = plan.NewStatus(plan.StatusStoreNotMatchRule)
	statusStoreNotMatchIsolation = plan.NewStatus(plan.StatusStoreNotMatchIsolation)

	// region filter status
	statusRegionPendingPeer                 = plan.NewStatus(plan.StatusRegionUnhealthy)
	statusRegionDownPeer                    = plan.NewStatus(plan.StatusRegionUnhealthy)
	statusRegionEmpty                       = plan.NewStatus(plan.StatusRegionEmpty)
	statusRegionNotMatchRule                = plan.NewStatus(plan.StatusRegionNotMatchRule)
	statusRegionNotReplicated               = plan.NewStatus(plan.StatusRegionNotReplicated)
	statusRegionWitnessPeer                 = plan.NewStatus(plan.StatusRegionNotMatchRule)
	statusRegionLeaderSendSnapshotThrottled = plan.NewStatus(plan.StatusRegionSendSnapshotThrottled)
	statusRegionAffinity                    = plan.NewStatus(plan.StatusRegionAffinity)
)
