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

package plan

// StatusCode is used to classify the plan result.
// And the sequence of status code represents the priority, the status code number is higher, the priority is higher.
type StatusCode int

const (
	// StatusOK represents the plan can be scheduled successfully.
	StatusOK StatusCode = iota
)

// normal status in most of situations.
const (
	// StatusStoreScoreDisallowed represents the plan is no need to be scheduled due to the score does meet the requirement.
	StatusStoreScoreDisallowed = iota + 100
	// StatusStoreAlreadyHasPeer represents the store is excluded due to the existed region peer.
	StatusStoreAlreadyHasPeer
	// StatusStoreNotMatchRule represents the placement rule cannot satisfy the requirement.
	StatusStoreNotMatchRule
)

// soft limitation
const (
	// StatusStoreSnapshotThrottled represents the store cannot be selected due to the snapshot limitation.
	StatusStoreSnapshotThrottled = iota + 200
	// StatusStorePendingPeerThrottled represents the store cannot be selected due to the pending peer limitation.
	StatusStorePendingPeerThrottled
	// StatusStoreAddLimitThrottled represents the store cannot be selected due to the add peer limitation.
	StatusStoreAddLimitThrottled
	// StatusStoreRemoveLimitThrottled represents the store cannot be selected due to the remove peer limitation.
	StatusStoreRemoveLimitThrottled
)

// config limitation
const (
	// StatusStoreRejectLeader represents the store is restricted by the special configuration. e.g. reject label setting, evict leader/slow store scheduler.
	StatusStoreRejectLeader = iota + 300
	// StatusStoreNotMatchIsolation represents the isolation cannot satisfy the requirement.
	StatusStoreNotMatchIsolation
)

// hard limitation
const (
	// StatusStoreBusy represents the store cannot be selected due to it is busy.
	StatusStoreBusy = iota + 400
	// StatusStoreRemoved represents the store cannot be selected due to it has been removed.
	StatusStoreRemoved
	// StatusStoreRemoving represents the data on the store is moving out.
	StatusStoreRemoving
	// StatusStoreDown represents the the store is in down state.
	StatusStoreDown
	// StatusStoreDisconnected represents the the store is in disconnected state.
	StatusStoreDisconnected
)

const (
	// StatusStoreLowSpace represents the store cannot be selected because it runs out of space.
	StatusStoreLowSpace = iota + 500
	// StatusStoreNotExisted represents the store cannot be found in PD.
	StatusStoreNotExisted
)

// TODO: define region status priority
const (
	// StatusRegionHot represents the region cannot be selected due to the heavy load.
	StatusRegionHot = iota + 1000
	// StatusRegionUnhealthy represents the region cannot be selected due to the region health.
	StatusRegionUnhealthy
	// StatusRegionEmpty represents the region cannot be selected due to the region is empty.
	StatusRegionEmpty
	// StatusRegionNotReplicated represents the region does not have enough replicas.
	StatusRegionNotReplicated
	// StatusRegionNotMatchRule represents the region does not match rule constraint.
	StatusRegionNotMatchRule
	// StatusRegionNoLeader represents the region has no leader.
	StatusRegionNoLeader
	// StatusNoTargetRegion represents nts the target region of merge operation cannot be found.
	StatusNoTargetRegion
	// StatusRegionLabelReject represents the plan conflicts with region label.
	StatusRegionLabelReject
	// StatusRegionSendSnapshotThrottled represents the plan conflicts with send snapshot.
	StatusRegionSendSnapshotThrottled
)

const (
	// StatusCreateOperatorFailed represents the plan can not create operators.
	StatusCreateOperatorFailed = iota + 2000
)

var statusText = map[StatusCode]string{
	StatusOK: "OK",

	// store in normal state usually
	StatusStoreScoreDisallowed: "StoreScoreDisallowed",
	StatusStoreAlreadyHasPeer:  "StoreAlreadyHasPeer",
	StatusStoreNotMatchRule:    "StoreNotMatchRule",

	// store is limited by soft constraint
	StatusStoreSnapshotThrottled:    "StoreSnapshotThrottled",
	StatusStorePendingPeerThrottled: "StorePendingPeerThrottled",
	StatusStoreAddLimitThrottled:    "StoreAddPeerThrottled",
	StatusStoreRemoveLimitThrottled: "StoreRemovePeerThrottled",

	// store is limited by specified configuration
	StatusStoreRejectLeader:      "StoreRejectLeader",
	StatusStoreNotMatchIsolation: "StoreNotMatchIsolation",

	// store is limited by hard constraint
	StatusStoreLowSpace:     "StoreLowSpace",
	StatusStoreRemoving:     "StoreRemoving",
	StatusStoreRemoved:      "StoreRemoved",
	StatusStoreDisconnected: "StoreDisconnected",
	StatusStoreDown:         "StoreDown",
	StatusStoreBusy:         "StoreBusy",

	StatusStoreNotExisted: "StoreNotExisted",

	// region
	StatusRegionHot:           "RegionHot",
	StatusRegionUnhealthy:     "RegionUnhealthy",
	StatusRegionEmpty:         "RegionEmpty",
	StatusRegionNotReplicated: "RegionNotReplicated",
	StatusRegionNotMatchRule:  "RegionNotMatchRule",
	StatusRegionNoLeader:      "RegionNoLeader",

	// non-filter
	StatusNoTargetRegion:    "NoTargetRegion",
	StatusRegionLabelReject: "RegionLabelReject",

	// operator
	StatusCreateOperatorFailed: "CreateOperatorFailed",
}

// StatusText turns the status code into string.
func StatusText(code StatusCode) string {
	return statusText[code]
}

// Status describes a plan's result.
type Status struct {
	StatusCode StatusCode
	// TODO: Try to indicate the user to do some actions through this field.
	DetailedReason string
}

// NewStatus create a new plan status.
func NewStatus(statusCode StatusCode, reason ...string) *Status {
	var detailedReason string
	if len(reason) != 0 {
		detailedReason = reason[0]
	}
	return &Status{
		StatusCode:     statusCode,
		DetailedReason: detailedReason,
	}
}

// IsOK returns true if the status code is StatusOK.
func (s *Status) IsOK() bool {
	return s.StatusCode == StatusOK
}

// Priority returns the priority of status for diagnose
func (s *Status) Priority() float32 {
	typePriority := int(s.StatusCode) / 100
	// This status is normal, we should set different priority.
	if typePriority == 1 {
		return float32(s.StatusCode) / 100.
	}
	// Otherwise, same type status will have the same priority.
	return float32(typePriority)
}

func (s *Status) String() string {
	return StatusText(s.StatusCode)
}

// IsNormal returns true if the status is normal.
func (s *Status) IsNormal() bool {
	return int(s.StatusCode)/10 == 10
}
