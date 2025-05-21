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

package progress

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
)

type mockPatrolRegionsDurationGetter struct {
	dur time.Duration
}

func (m *mockPatrolRegionsDurationGetter) GetPatrolRegionsDuration() time.Duration {
	return m.dur
}

func TestProgress(t *testing.T) {
	var (
		re             = require.New(t)
		storeID        = uint64(1)
		storeID2       = uint64(2)
		updateInterval = time.Second
		m              = NewManager(&mockPatrolRegionsDurationGetter{10 * time.Second}, updateInterval)
	)

	// test add progress
	m.addProgress(storeID, preparingAction, 10, 100)
	p := m.GetProgressByStoreID(storeID)
	re.Equal(preparingAction, p.Action)
	re.Equal(0.1, p.ProgressPercent)
	re.Equal(math.MaxFloat64, p.LeftSecond)
	re.Equal(0.0, p.CurrentSpeed)
	p2 := m.GetProgressByStoreID(storeID)
	re.Equal(p2, p)

	m.updateStoreProgress(storeID2, preparingAction, 20, 100)
	p = m.GetProgressByStoreID(storeID2)
	re.Equal(preparingAction, p.Action)
	re.Equal(0.2, p.ProgressPercent)
	re.Equal(math.MaxFloat64, p.LeftSecond)
	re.Equal(0.0, p.CurrentSpeed)
	p2 = m.GetProgressByStoreID(storeID2)
	re.Equal(p2, p)

	// test update progress
	m.updateStoreProgress(storeID, preparingAction, 30, 100)
	p = m.GetProgressByStoreID(storeID)
	re.Equal(preparingAction, p.Action)
	re.Equal(0.3, p.ProgressPercent)
	re.Equal(20.0, p.CurrentSpeed) 
	re.Equal(3.50, p.LeftSecond)

	m.updateStoreProgress(storeID2, preparingAction, 30, 100)
	p = m.GetProgressByStoreID(storeID2)
	re.Equal(preparingAction, p.Action)
	re.Equal(0.3, p.ProgressPercent)
	re.Equal(10.0, p.CurrentSpeed) 
	re.Equal(7.0, p.LeftSecond)

	// test gc progress
	m.markProgressAsFinished(storeID)
	failpoint.Enable("github.com/tikv/pd/pkg/progress/gcExpiredTime", `return("100ms")`)
	time.Sleep(200 * time.Millisecond)
	m.gcCompletedProgress()
	re.Nil(m.GetProgressByStoreID(storeID))
	failpoint.Disable("github.com/tikv/pd/pkg/progress/gcExpiredTime")
}

func TestOnlineAndOffline(t *testing.T) {
	var (
		re             = require.New(t)
		updateInterval = time.Second
		m              = NewManager(&mockPatrolRegionsDurationGetter{10 * time.Second}, updateInterval)
	)

	testOnline := func(sourceState, targetState metapb.NodeState) {
		store := core.NewStoreInfo(&metapb.Store{
			Id:        1,
			NodeState: sourceState,
		})
		m.UpdateProgress(store, 10, 100)
		p := m.GetProgressByStoreID(store.GetID())
		re.Equal(preparingAction, p.Action)
		re.Equal(0.1, p.ProgressPercent)
		re.Equal(math.MaxFloat64, p.LeftSecond)
		re.Equal(0.0, p.CurrentSpeed)

		m.UpdateProgress(store, 20, 100)
		p = m.GetProgressByStoreID(store.GetID())
		re.Equal(preparingAction, p.Action)
		re.Equal(0.2, p.ProgressPercent)
		re.Equal(8*updateInterval.Seconds(), p.LeftSecond)
		re.Equal(10.0, p.CurrentSpeed)

		store = store.Clone(core.SetNodeState(targetState))
		m.UpdateProgress(store, 101, 100)
		p = m.GetProgressByStoreID(store.GetID())
		re.Nil(p)
		p = m.completedProgress[store.GetID()].Progress
		re.Equal(preparingAction, p.Action)
		re.Equal(1.0, p.ProgressPercent)
		re.Equal(0.0, p.LeftSecond)
		re.Equal(45.0, p.CurrentSpeed)
	}

	testOffline := func(sourceState, targetState metapb.NodeState) {
		store := core.NewStoreInfo(&metapb.Store{
			Id:        1,
			NodeState: sourceState,
		})
		m.UpdateProgress(store, 100, 0)
		p := m.GetProgressByStoreID(store.GetID())
		re.Equal(removingAction, p.Action)
		re.Equal(0.0, p.ProgressPercent)
		re.Equal(math.MaxFloat64, p.LeftSecond)
		re.Equal(0.0, p.CurrentSpeed)

		m.UpdateProgress(store, 80, 0)
		p = m.GetProgressByStoreID(store.GetID())
		re.Equal(removingAction, p.Action)
		re.Equal(0.2, p.ProgressPercent)
		re.Equal(4*updateInterval.Seconds(), p.LeftSecond)
		re.Equal(20.0, p.CurrentSpeed)

		store = store.Clone(core.SetNodeState(targetState))
		m.UpdateProgress(store, 0, 0)
		p = m.GetProgressByStoreID(store.GetID())
		re.Nil(p)
		p = m.completedProgress[store.GetID()].Progress
		re.Equal(removingAction, p.Action)
		re.Equal(1.0, p.ProgressPercent)
		re.Equal(0.0, p.LeftSecond)
		re.Equal(50.0, p.CurrentSpeed)
	}

	testOnline(metapb.NodeState_Preparing, metapb.NodeState_Serving)
	testOffline(metapb.NodeState_Removing, metapb.NodeState_Removed)
	// test twice
	testOnline(metapb.NodeState_Preparing, metapb.NodeState_Serving)
	testOffline(metapb.NodeState_Removing, metapb.NodeState_Removed)

	// test online, skip serving and offline directly
	store := core.NewStoreInfo(&metapb.Store{
		Id:        1,
		NodeState: metapb.NodeState_Preparing,
	})

	m.UpdateProgress(store, 10, 100)
	p := m.GetProgressByStoreID(store.GetID())
	re.Equal(preparingAction, p.Action)
	re.Equal(0.1, p.ProgressPercent)
	re.Equal(math.MaxFloat64, p.LeftSecond)
	re.Equal(0.0, p.CurrentSpeed)

	store = store.Clone(core.SetNodeState(metapb.NodeState_Removing))
	m.UpdateProgress(store, 100, 0)
	p = m.GetProgressByStoreID(store.GetID())
	re.NotNil(p)
	re.Equal(removingAction, p.Action)
	re.Equal(0.0, p.ProgressPercent)
	re.Equal(math.MaxFloat64, p.LeftSecond)
	re.Equal(0.0, p.CurrentSpeed)

	// preparing is completed
	p = m.completedProgress[store.GetID()].Progress
	re.Equal(preparingAction, p.Action)
	re.Equal(1.0, p.ProgressPercent)
	re.Equal(0.0, p.LeftSecond)
	re.Equal(90.0, p.CurrentSpeed)
}
