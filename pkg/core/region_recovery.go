// Copyright 2026 TiKV Project Authors.
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
	"github.com/gogo/protobuf/proto"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

// LogOnlyProtocolVersion is the supported LogOnly peer recovery protocol.
const LogOnlyProtocolVersion uint32 = 2

// GetRecoveryStatus returns the leader's explicit, term-bound recovery report.
// A nil report retains legacy behavior unless the Store declares protocol v2.
func (r *RegionInfo) GetRecoveryStatus() *pdpb.RegionRecoveryStatus {
	return r.recoveryStatus
}

// GetRequiredReaderProtocolVersion returns the durable DFS reader requirement.
// Unlike recovery readiness, this requirement remains after returning to 3F.
func (r *RegionInfo) GetRequiredReaderProtocolVersion() uint32 {
	return r.recoveryStatus.GetRequiredReaderProtocolVersion()
}

// InheritReaderProtocolVersion preserves the highest observed durable reader
// requirement. Only that field is inherited: readiness, session and size
// provenance must describe the current report. Call before publishing r.
func (r *RegionInfo) InheritReaderProtocolVersion(origin *RegionInfo) {
	if origin == nil || origin.GetRequiredReaderProtocolVersion() <= r.GetRequiredReaderProtocolVersion() {
		return
	}
	status := &pdpb.RegionRecoveryStatus{}
	if r.recoveryStatus != nil {
		status = proto.Clone(r.recoveryStatus).(*pdpb.RegionRecoveryStatus)
	}
	status.RequiredReaderProtocolVersion = origin.GetRequiredReaderProtocolVersion()
	r.recoveryStatus = status
}

// IsRecoveringLeader reports whether the Raft leader is restoring a Full peer.
func (r *RegionInfo) IsRecoveringLeader() bool {
	return r.recoveryStatus.GetState() == pdpb.RegionRecoveryState_RECOVERING_LEADER
}

// IsRecoverySchedulingBlocked rejects discretionary scheduling while the data
// service is unavailable or size estimates have only historical provenance.
// Replica repair remains separate from this scheduling eligibility check.
func (r *RegionInfo) IsRecoverySchedulingBlocked() bool {
	status := r.recoveryStatus
	return status != nil && (status.GetState() != pdpb.RegionRecoveryState_NORMAL ||
		!status.GetDataServiceReady() || status.GetSizeSource() != pdpb.RegionSizeSource_SIZE_CURRENT)
}

// NormalizeRegionRecoveryStatus fails closed when a v2 Store omits its explicit
// status. An absent report from an older Store continues to mean legacy mode.
func NormalizeRegionRecoveryStatus(region *RegionInfo, leaderStore *StoreInfo) *RegionInfo {
	if region.GetRecoveryStatus() != nil || leaderStore == nil ||
		leaderStore.GetMeta().GetLogOnlyProtocolVersion() < LogOnlyProtocolVersion {
		return region
	}
	return region.Clone(WithRecoveryStatus(&pdpb.RegionRecoveryStatus{}))
}
