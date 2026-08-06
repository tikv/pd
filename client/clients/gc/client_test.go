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

package gc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/client/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestGCBarrierInfoExpiration(t *testing.T) {
	re := require.New(t)

	now := time.Now()
	b := NewGCBarrierInfo("b", 1, time.Second, now)
	re.False(b.isExpiredImpl(now.Add(-time.Second)))
	re.False(b.isExpiredImpl(now))
	re.False(b.isExpiredImpl(now.Add(time.Millisecond * 999)))
	re.False(b.isExpiredImpl(now.Add(time.Second)))
	re.True(b.isExpiredImpl(now.Add(time.Second + time.Millisecond)))
	re.True(b.isExpiredImpl(now.Add(time.Hour * 24 * 365 * 10)))

	b = NewGCBarrierInfo("b", 1, TTLNeverExpire, now)
	re.False(b.IsExpired())
	re.False(b.isExpiredImpl(now))
	re.False(b.isExpiredImpl(now.Add(time.Hour * 24 * 365 * 10)))

	b1 := NewGlobalGCBarrierInfo("b", 1, time.Second, now)
	re.False(b1.IsExpired())
	re.False(b1.isExpiredImpl(now.Add(-time.Second)))
	re.False(b1.isExpiredImpl(now))
	re.False(b1.isExpiredImpl(now.Add(time.Millisecond * 999)))
	re.False(b1.isExpiredImpl(now.Add(time.Second)))
	re.True(b1.isExpiredImpl(now.Add(time.Second + time.Millisecond)))
	re.True(b1.isExpiredImpl(now.Add(time.Hour * 24 * 365 * 10)))

	b1 = NewGlobalGCBarrierInfo("b", 1, TTLNeverExpire, now)
	re.False(b1.isExpiredImpl(now))
	re.False(b1.isExpiredImpl(now.Add(time.Hour * 24 * 365 * 10)))
}

func TestGCStateAccessors(t *testing.T) {
	re := require.New(t)

	state := NewGCStateWithoutGCBarriers(1, 2, 3)
	re.False(state.HasGCBarriers())
	barriers, err := state.GetGCBarriers()
	re.Error(err)
	re.Nil(barriers)

	state = NewGCStateWithGCBarriers(1, 2, 3, []*GCBarrierInfo{
		NewGCBarrierInfo("b1", 4, time.Second, time.Now()),
	})
	re.True(state.HasGCBarriers())
	barriers, err = state.GetGCBarriers()
	re.NoError(err)
	re.Len(barriers, 1)
	re.Equal("b1", barriers[0].BarrierID)
}

func TestClusterGCStatesAccessors(t *testing.T) {
	re := require.New(t)

	state := NewGCStateWithoutGCBarriers(1, 2, 3)
	clusterState := NewClusterGCStatesWithoutGlobalGCBarriers(map[uint32]GCState{1: state})
	re.False(clusterState.HasGlobalGCBarriers())
	barriers, err := clusterState.GetGlobalGCBarriers()
	re.Error(err)
	re.Nil(barriers)

	clusterState = NewClusterGCStatesWithGlobalGCBarriers(map[uint32]GCState{1: state}, []*GlobalGCBarrierInfo{
		NewGlobalGCBarrierInfo("b1", 4, time.Second, time.Now()),
	})
	re.True(clusterState.HasGlobalGCBarriers())
	barriers, err = clusterState.GetGlobalGCBarriers()
	re.NoError(err)
	re.Len(barriers, 1)
	re.Equal("b1", barriers[0].BarrierID)
}
