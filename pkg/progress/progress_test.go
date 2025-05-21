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
	"fmt"
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

func TestDynamicPatrolRegionsDuration(t *testing.T) {
	var (
		re = require.New(t)
		// Corresponding window capacity is 121, window length is 11
		updateInterval = time.Minute
		store          = core.NewStoreInfo(&metapb.Store{
			Id:        1,
			NodeState: metapb.NodeState_Removing,
		})
		mockGetter = &mockPatrolRegionsDurationGetter{10 * time.Second}
		m          = NewManager(mockGetter, updateInterval)
	)

	checkSecond := func(seconds, expected float64) {
		re.LessOrEqual(math.Abs(seconds/updateInterval.Seconds()-expected), 0.01,
			fmt.Sprintf("expected %f", seconds/updateInterval.Seconds()))
	}
	checkSpeed := func(speed, expected float64) {
		re.LessOrEqual(math.Abs(speed*updateInterval.Seconds()-expected), 0.01,
			fmt.Sprintf("expected %f", speed*updateInterval.Seconds()))
	}

	currentRegionSize := 1000.0
	for range 121 {
		m.UpdateProgress(store, currentRegionSize, 0)
		currentRegionSize--
	}
	p := m.GetProgressByStoreID(store.GetID())
	re.Equal(removingAction, p.Action)
	re.Equal(0.12, p.ProgressPercent)
	checkSecond(p.LeftSecond, 880.0)
	checkSpeed(p.CurrentSpeed, 1)

	// PatrolRegion has not scanned any offline peers within 5 minutes
	currentRegionSize++
	for range 5 {
		m.UpdateProgress(store, currentRegionSize, 0)
	}

	p = m.GetProgressByStoreID(store.GetID())
	// Speed becomes slower
	checkSecond(p.LeftSecond, 1760.0)
	checkSpeed(p.CurrentSpeed, 0.5)

	// Before it was 10 minutes(minSpeedCalculationWindow), window length is 10,
	// Now it is 19 minutes(maxSpeedCalculationWindow), window length is 20.
	mockGetter.dur = 19 * time.Minute
	for range 4 { // why 4? because I want to keep ten identical values in the history.
		m.UpdateProgress(store, currentRegionSize, 0)
	}
	p = m.GetProgressByStoreID(store.GetID())
	// Speed becomes stable, because the patrol region duration is larger.
	checkSecond(p.LeftSecond, 1672.0)
	checkSpeed(p.CurrentSpeed, 0.52631)
	re.Equal(20, m.progresses[store.GetID()].currentWindowLength)
	front, back := 110.0, 120.0
	re.Equal(front, m.progresses[store.GetID()].front.Value.(float64))
	re.Equal(back, m.progresses[store.GetID()].history.Back().Value.(float64))

	for i := range 10 {
		currentRegionSize--
		m.UpdateProgress(store, currentRegionSize, 0)
		p = m.GetProgressByStoreID(store.GetID())

		front++
		back++
		re.Equal(front, m.progresses[store.GetID()].front.Value.(float64), i)
		re.Equal(back, m.progresses[store.GetID()].history.Back().Value.(float64), i)
		// Speed becomes stable.
		checkSpeed(p.CurrentSpeed, 0.52631)
	}
}
