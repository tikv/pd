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

package pd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

func TestPBToGCStatePreservesKeyspaceLevelGC(t *testing.T) {
	requestStart := time.Unix(100, 0)
	for _, testCase := range []struct {
		name              string
		isKeyspaceLevelGC bool
		excludeBarriers   bool
	}{
		{name: "keyspace-level-with-barriers", isKeyspaceLevelGC: true},
		{
			name:              "keyspace-level-without-barriers",
			isKeyspaceLevelGC: true,
			excludeBarriers:   true,
		},
		{name: "unified-with-barriers", isKeyspaceLevelGC: false},
		{
			name:              "unified-without-barriers",
			isKeyspaceLevelGC: false,
			excludeBarriers:   true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pbState := &pdpb.GCState{
				KeyspaceScope:     &pdpb.KeyspaceScope{KeyspaceId: 42},
				IsKeyspaceLevelGc: testCase.isKeyspaceLevelGC,
				TxnSafePoint:      100,
				GcSafePoint:       90,
				GcBarriers: []*pdpb.GCBarrierInfo{
					{BarrierId: "backup", BarrierTs: 95, TtlSeconds: 60},
				},
			}

			state := pbToGCState(
				pbState,
				requestStart,
				testCase.excludeBarriers,
			)
			require.Equal(t, testCase.isKeyspaceLevelGC,
				state.IsKeyspaceLevelGC)
			require.Equal(t, uint32(42), state.KeyspaceID)
			require.Equal(t, uint64(100), state.TxnSafePoint)
			require.Equal(t, uint64(90), state.GCSafePoint)
			require.Equal(t, !testCase.excludeBarriers,
				state.HasGCBarriers())
		})
	}
}
