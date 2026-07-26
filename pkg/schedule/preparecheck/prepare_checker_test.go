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

package preparecheck

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestRunIfPrepared(t *testing.T) {
	re := require.New(t)
	checker := NewChecker(func() (int, error) {
		return 0, nil
	})

	ran := false
	re.False(checker.RunIfPrepared(func() {
		ran = true
	}))
	re.False(ran)

	checker.SetPrepared()
	re.True(checker.RunIfPrepared(func() {
		ran = true
	}))
	re.True(ran)

	ran = false
	checker.ResetPrepared()
	re.False(checker.RunIfPrepared(func() {
		ran = true
	}))
	re.False(ran)
}

func TestResetPreparedAndRunFencesPrepareCheck(t *testing.T) {
	re := require.New(t)
	var resetFinished atomic.Bool
	var checkedBeforeResetFinished atomic.Bool
	checker := NewChecker(func() (int, error) {
		if !resetFinished.Load() {
			checkedBeforeResetFinished.Store(true)
		}
		return 0, nil
	})
	checker.SetPrepared()

	resetStarted := make(chan struct{})
	finishReset := make(chan struct{})
	resetDone := make(chan struct{})
	go func() {
		checker.ResetPreparedAndRun(func() {
			close(resetStarted)
			<-finishReset
			resetFinished.Store(true)
		})
		close(resetDone)
	}()
	<-resetStarted

	checkStarted := make(chan struct{})
	checkDone := make(chan bool, 1)
	go func() {
		close(checkStarted)
		checkDone <- checker.Check(core.NewBasicCluster())
	}()
	<-checkStarted
	time.Sleep(50 * time.Millisecond)
	completedEarly := false
	select {
	case <-checkDone:
		completedEarly = true
	default:
	}

	close(finishReset)
	<-resetDone
	if completedEarly {
		re.Fail("prepare check completed before reset callback")
		return
	}
	re.True(<-checkDone)
	re.False(checkedBeforeResetFinished.Load())
}
