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

package schedulers

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
)

type shutdownTestScheduler struct {
	Scheduler
	allow func() bool
	clean func()
}

func (s *shutdownTestScheduler) IsScheduleAllowed(sche.SchedulerCluster) bool {
	return s.allow()
}

func (s *shutdownTestScheduler) CleanConfig(sche.SchedulerCluster) {
	s.clean()
}

func TestRemoveSchedulerWaitsForLoopAndCleanup(t *testing.T) {
	re := require.New(t)
	clean, _, tc, oc := prepareSchedulersTest()
	t.Cleanup(clean)
	ctx, cancel := context.WithCancel(t.Context())
	store := storage.NewStorageWithMemoryBackend()
	c := NewController(ctx, tc, store, oc)
	scheduler, err := CreateScheduler(types.ShuffleLeaderScheduler, oc, store,
		ConfigSliceDecoder(types.ShuffleLeaderScheduler, []string{"", ""}))
	re.NoError(err)
	name := scheduler.GetName()

	loopEntered, resumeLoop := make(chan struct{}), make(chan struct{})
	cleanupEntered, resumeCleanup := make(chan struct{}), make(chan struct{})
	releaseLoop := sync.OnceFunc(func() { close(resumeLoop) })
	releaseCleanup := sync.OnceFunc(func() { close(resumeCleanup) })
	t.Cleanup(func() {
		cancel()
		releaseLoop()
		releaseCleanup()
		c.Wait()
	})
	writes := make(chan error, 2)
	writeFromLoop := sync.OnceFunc(func() {
		close(loopEntered)
		<-resumeLoop
		writes <- store.SaveSchedulerConfig(name, []byte(`"loop"`))
	})
	s := &shutdownTestScheduler{
		Scheduler: scheduler,
		allow: func() bool {
			// Like balance-range checkJob, this may write before Schedule's
			// cancellation check and must finish before config removal.
			writeFromLoop()
			return false
		},
		clean: func() {
			close(cleanupEntered)
			<-resumeCleanup
			writes <- store.SaveSchedulerConfig(name, []byte(`"cleanup"`))
		},
	}
	re.NoError(c.AddScheduler(s))
	sc := c.GetScheduler(name)
	initial, err := store.LoadSchedulerConfig(name)
	re.NoError(err)
	re.NotEmpty(initial)
	wait := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for scheduler shutdown progress")
		}
	}
	wait(loopEntered)
	removed := make(chan error, 1)
	go func() { removed <- c.RemoveScheduler(name) }()
	wait(sc.Ctx().Done())

	// Cancellation must not delete the config while the old loop can write.
	value, err := store.LoadSchedulerConfig(name)
	re.NoError(err)
	re.Equal(initial, value)
	releaseLoop()
	wait(cleanupEntered)
	re.NoError(<-writes)
	value, err = store.LoadSchedulerConfig(name)
	re.NoError(err)
	re.Equal(`"loop"`, value)
	select {
	case err := <-removed:
		t.Fatalf("removal returned before cleanup finished: %v", err)
	default:
	}

	releaseCleanup()
	select {
	case err := <-removed:
		re.NoError(err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for scheduler removal")
	}
	re.NoError(<-writes)
	c.Wait()
	value, err = store.LoadSchedulerConfig(name)
	re.NoError(err)
	re.Empty(value, "neither the loop nor cleanup may resurrect the config")
	re.Nil(c.GetScheduler(name))
}
