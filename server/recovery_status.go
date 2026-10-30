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

package server

import (
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
)

// Preserve the producer's readiness and size provenance, while forwarding any
// higher durable reader requirement already observed by this PD. Never mutate
// RegionInfo objects that may already have been published to the cache.
func recoveryStatusForForwarding(report *core.RegionInfo, leaderStore *core.StoreInfo, cached *core.RegionInfo) *pdpb.RegionRecoveryStatus {
	report = core.NormalizeRegionRecoveryStatus(report, leaderStore).Clone()
	report.InheritReaderProtocolVersion(cached)
	return report.GetRecoveryStatus()
}
