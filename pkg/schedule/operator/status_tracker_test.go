// Copyright 2019 TiKV Project Authors.
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

package operator

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCreate(t *testing.T) {
	re := require.New(t)
	before := time.Now()
	trk := NewOpStatusTracker()
	re.Equal(CREATED, trk.Status())
	re.Equal(trk.ReachTimeOf(CREATED), trk.ReachTime())
	checkTimeOrder(re, before, trk.ReachTime(), time.Now())
	checkReachTime(re, &trk, CREATED)
}

func TestNonEndTrans(t *testing.T) {
	re := require.New(t)
	{
		trk := NewOpStatusTracker()
		checkInvalidTrans(re, &trk, SUCCESS, REPLACED, TIMEOUT)
		checkValidTrans(re, &trk, STARTED)
		checkInvalidTrans(re, &trk, EXPIRED)
		checkValidTrans(re, &trk, SUCCESS)
		checkReachTime(re, &trk, CREATED, STARTED, SUCCESS)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(re, &trk, CANCELED)
		checkReachTime(re, &trk, CREATED, CANCELED)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(re, &trk, STARTED)
		checkValidTrans(re, &trk, CANCELED)
		checkReachTime(re, &trk, CREATED, STARTED, CANCELED)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(re, &trk, STARTED)
		checkValidTrans(re, &trk, REPLACED)
		checkReachTime(re, &trk, CREATED, STARTED, REPLACED)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(re, &trk, EXPIRED)
		checkReachTime(re, &trk, CREATED, EXPIRED)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(re, &trk, STARTED)
		checkValidTrans(re, &trk, TIMEOUT)
		checkReachTime(re, &trk, CREATED, STARTED, TIMEOUT)
	}
}

func TestEndStatusTrans(t *testing.T) {
	re := require.New(t)
	allStatus := make([]OpStatus, 0, statusCount)
	for st := OpStatus(0); st < statusCount; st++ {
		allStatus = append(allStatus, st)
	}
	for from := firstEndStatus; from < statusCount; from++ {
		trk := NewOpStatusTracker()
		trk.current = from
		re.True(trk.IsEnd())
		checkInvalidTrans(re, &trk, allStatus...)
	}
}

func TestCheckExpired(t *testing.T) {
	re := require.New(t)
	{
		// Not expired
		before := time.Now()
		trk := NewOpStatusTracker()
		after := time.Now()
		re.False(trk.CheckExpired(10 * time.Second))
		re.Equal(CREATED, trk.Status())
		checkTimeOrder(re, before, trk.ReachTime(), after)
	}
	{
		// Expired but status not changed
		trk := NewOpStatusTracker()
		trk.setTime(CREATED, time.Now().Add(-10*time.Second))
		re.True(trk.CheckExpired(5 * time.Second))
		re.Equal(EXPIRED, trk.Status())
	}
	{
		// Expired and status changed
		trk := NewOpStatusTracker()
		before := time.Now()
		re.True(trk.To(EXPIRED))
		after := time.Now()
		re.True(trk.CheckExpired(0))
		re.Equal(EXPIRED, trk.Status())
		checkTimeOrder(re, before, trk.ReachTime(), after)
	}
}

func TestCheckStepTimeout(t *testing.T) {
	re := require.New(t)
	testdata := []struct {
		step   OpStep
		start  time.Time
		status OpStatus
	}{{
		step:   AddLearner{},
		start:  time.Now().Add(-(SlowStepWaitTime - time.Second)),
		status: STARTED,
	}, {
		step:   AddLearner{},
		start:  time.Now().Add(-(SlowStepWaitTime + time.Second)),
		status: TIMEOUT,
	}}

	for _, v := range testdata {
		// Timeout and status changed
		trk := NewOpStatusTracker()
		trk.To(STARTED)
		trk.reachTimes[STARTED] = v.start
		re.Equal(v.status == TIMEOUT, trk.CheckTimeout(SlowStepWaitTime))
		re.Equal(v.status, trk.Status())
	}
}

func checkTimeOrder(re *require.Assertions, t1, t2, t3 time.Time) {
	re.True(t1.Before(t2))
	re.True(t3.After(t2))
}

func checkValidTrans(re *require.Assertions, trk *OpStatusTracker, st OpStatus) {
	before := time.Now()
	re.True(trk.To(st))
	re.Equal(st, trk.Status())
	re.Equal(trk.ReachTimeOf(st), trk.ReachTime())
	checkTimeOrder(re, before, trk.ReachTime(), time.Now())
}

func checkInvalidTrans(re *require.Assertions, trk *OpStatusTracker, sts ...OpStatus) {
	origin := trk.Status()
	originTime := trk.ReachTime()
	sts = append(sts, statusCount, statusCount+1, statusCount+10)
	for _, st := range sts {
		re.False(trk.To(st))
		re.Equal(origin, trk.Status())
		re.Equal(originTime, trk.ReachTime())
	}
}

func checkReachTime(re *require.Assertions, trk *OpStatusTracker, reached ...OpStatus) {
	reachedMap := make(map[OpStatus]struct{}, len(reached))
	for _, st := range reached {
		re.False(trk.ReachTimeOf(st).IsZero())
		reachedMap[st] = struct{}{}
	}
	for st := OpStatus(0); st <= statusCount+10; st++ {
		if _, ok := reachedMap[st]; ok {
			continue
		}
		re.True(trk.ReachTimeOf(st).IsZero())
	}
}

func TestAdditionalInfoConcurrent(t *testing.T) {
	op := NewOperator("test", "test", 0, nil, OpAdmin, 0)

	var wg sync.WaitGroup
	for i := range 1000 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i)
			op.SetAdditionalInfo(key, value)
			if op.GetAdditionalInfo(key) != value {
				t.Errorf("unexpected value for key %s", key)
			}
		}(i)
	}
	wg.Wait()

	if logInfo := op.LogAdditionalInfo(); logInfo == "" {
		t.Error("LogAdditionalInfo returned an empty string")
	}
}
