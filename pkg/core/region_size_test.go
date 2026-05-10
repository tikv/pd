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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

func TestRegionSizeKB(t *testing.T) {
	re := require.New(t)

	// Test 1: Old heartbeat with bytes only (approximate_size = 512KB)
	heartbeat := &pdpb.RegionHeartbeatRequest{
		Region:          &metapb.Region{Id: 1},
		ApproximateSize: 512 * 1024,
	}
	region := RegionFromHeartbeat(heartbeat, 0)
	// approximateSize (MiB) should be 0
	re.Equal(int64(0), region.GetApproximateSize())
	// approximateSizeKb (KiB) should be 512
	re.Equal(int64(512), region.GetApproximateSizeKb())

	// Test 2: New heartbeat with approximate_size_kb = 512
	heartbeat = &pdpb.RegionHeartbeatRequest{
		Region:            &metapb.Region{Id: 1},
		ApproximateSizeKb: 512,
	}
	region = RegionFromHeartbeat(heartbeat, 0)
	re.Equal(int64(0), region.GetApproximateSize())
	re.Equal(int64(512), region.GetApproximateSizeKb())

	// Test 3: Region size < 1 KiB should be floored to 1 KiB (EmptyRegionApproximateSize)
	heartbeat = &pdpb.RegionHeartbeatRequest{
		Region:          &metapb.Region{Id: 1},
		ApproximateSize: 100, // 100 bytes
	}
	region = RegionFromHeartbeat(heartbeat, 0)
	re.Equal(int64(0), region.GetApproximateSize())
	re.Equal(int64(1), region.GetApproximateSizeKb())

	// Test 4: Truly empty region (size=0) should stay 0
	heartbeat = &pdpb.RegionHeartbeatRequest{
		Region:          &metapb.Region{Id: 1},
		ApproximateSize: 0,
	}
	region = RegionFromHeartbeat(heartbeat, 0)
	re.Equal(int64(0), region.GetApproximateSize())
	re.Equal(int64(0), region.GetApproximateSizeKb())
}
