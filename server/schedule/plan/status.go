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

import "fmt"

// StatusCode is used to classify the plan result.
type StatusCode int

const (
	// StatusOK represents the plan can be scheduled successfully.
	StatusOK StatusCode = iota
	// StatusNoNeed represents the plan is no need to be scheduled.
	StatusNoNeed
	// StatusPaused represents the scheduler or checker is paused.
	StatusPaused

	// StatusStoreThrottled represents the store cannot be selected due to the limitation.
	StatusStoreThrottled
	// StatusStoreUnavailable represents the store cannot be selected due to it's state.
	StatusStoreUnavailable
	// StatusStoreLowSpace represents the store cannot be selected because it runs out of space.
	StatusStoreLowSpace
	// StatusStoreDraining represents the data on the store is moving out.
	StatusStoreDraining
	// StatusStoreBlocked represents the store is restricted by the special configuration.
	StatusStoreBlocked
	// StatusStoreExcluded represents the store is excluded due to the existed or unhealthy region peer.
	StatusStoreExcluded

	// StatusRegionHot represents the region cannot be selected due to the heavy load.
	StatusRegionHot
	// StatusRegionUnhealthy represents the region cannot be selected due to the region health.
	StatusRegionUnhealthy
	// StatusRegionEmpty represents the region cannot be selected due to the region is empty.
	StatusRegionEmpty
	// StatusRegionNotReplicated represents the region does not have enough replicas.
	StatusRegionNotReplicated

	// StatusLabelNotMatch represents the location label of placement rule is not match the store's label.
	StatusLabelNotMatch
	// StatusRuleNotMatch represents the placement rule cannot satisfy the requirement.
	StatusRuleNotMatch
	// StatusIsolationNotMatch represents the isolation cannot satisfy the requirement.
	StatusIsolationNotMatch

	// TODO: The below status is not used for now. Once it is used, Please remove this comment.

	// StatusStoreNotExisted represents the store cannot be found in PD.
	StatusStoreNotExisted
	// StatusNoTargetRegion represents the target region of merge operation cannot be found.
	StatusNoTargetRegion
	// StatusRegionLabelReject represents the plan conflicts with region label.
	StatusRegionLabelReject
	// StatusCreateOperatorFailed represents the plan stops at creating operator.
	StatusCreateOperatorFailed
)

var statusText = map[StatusCode]string{
	StatusOK:     "OK",
	StatusNoNeed: "No Need",
	StatusPaused: "Paused",

	// store
	StatusStoreThrottled:   "Store Throttled",
	StatusStoreUnavailable: "Store Unavailable",
	StatusStoreLowSpace:    "Store Low Space",
	StatusStoreDraining:    "Store Draining",
	StatusStoreBlocked:     "Store Blocked",
	StatusStoreExcluded:    "Region Excluded",

	// region
	StatusRegionHot:           "Region Hot",
	StatusRegionUnhealthy:     "Region Unhealthy",
	StatusRegionEmpty:         "Region Empty",
	StatusRegionNotReplicated: "Region Not Replicated",

	StatusLabelNotMatch:     "Label Not Match",
	StatusRuleNotMatch:      "Rule Not Match",
	StatusIsolationNotMatch: "Isolation Not Match",

	// non-filter
	StatusStoreNotExisted:   "Store Not Existed",
	StatusNoTargetRegion:    "No Target Region",
	StatusRegionLabelReject: "Region Label Reject",
}

// StatusText turns the status code into string.
func StatusText(code StatusCode) string {
	return statusText[code]
}

// Status describes a plan's result.
type Status struct {
	StatusCode     StatusCode
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

func (s Status) String() string {
	return fmt.Sprintf("%s, %s", StatusText(s.StatusCode), s.DetailedReason)
}
