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
	// StatusScoreAlmostSame represents the plan is no need to be scheduled due to the score is almost the same.
	StatusScoreAlmostSame = iota + 100
	// StatusStoreAlreadyHasPeer represents the store is excluded due to the existed region peer.
	StatusStoreAlreadyHasPeer
	// StatusNotMatchRule represents the placement rule cannot satisfy the requirement.
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
	// StatusNotMatchIsolation represents the isolation cannot satisfy the requirement.
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
	// StatusNoTargetRegion represents nts the target region of merge operation cannot be found.
	StatusNoTargetRegion
	// StatusRegionLabelReject represents the plan conflicts with region label.
	StatusRegionLabelReject
)

var statusText = map[StatusCode]string{
	StatusOK: "OK",

	// store in normal state usually
	StatusScoreAlmostSame:     "Store Score Almost Same",
	StatusStoreAlreadyHasPeer: "Store Already Has Peer",
	StatusStoreNotMatchRule:   "Store Not Match Rule",

	// store is limited by soft constraint
	StatusStoreSnapshotThrottled:    "Store Snapshot Throttled",
	StatusStorePendingPeerThrottled: "Store Pending Peer Throttled",
	StatusStoreAddLimitThrottled:    "Store Add Peer Throttled",
	StatusStoreRemoveLimitThrottled: "Store Remove Peer Throttled",

	// store is limited by specified configuration
	StatusStoreRejectLeader:      "Store Reject Leader",
	StatusStoreNotMatchIsolation: "Store Not Match Isolation",

	// store is limited by hard constraint
	StatusStoreLowSpace:     "Store Low Space",
	StatusStoreRemoving:     "Store Removing",
	StatusStoreRemoved:      "Store Removed",
	StatusStoreDisconnected: "Store Disconnected",
	StatusStoreDown:         "Store Down",
	StatusStoreBusy:         "Store Busy",

	StatusStoreNotExisted: "Store Not Existed",

	// region
	StatusRegionHot:           "Region Hot",
	StatusRegionUnhealthy:     "Region Unhealthy",
	StatusRegionEmpty:         "Region Empty",
	StatusRegionNotReplicated: "Region Not Replicated",

	// non-filter
	StatusNoTargetRegion:    "No Target Region",
	StatusRegionLabelReject: "Region Label Reject",
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
func NewStatus(statusCode StatusCode, reason ...string) Status {
	var detailedReason string
	if len(reason) != 0 {
		detailedReason = reason[0]
	}
	return Status{
		StatusCode:     statusCode,
		DetailedReason: detailedReason,
	}
}

// IsOK returns true if the status code is StatusOK.
func (s Status) IsOK() bool {
	return s.StatusCode == StatusOK
}

func (s Status) Priority() int {
	return int(s.StatusCode) / 100
}

func (s Status) String() string {
	return StatusText(s.StatusCode)
}
