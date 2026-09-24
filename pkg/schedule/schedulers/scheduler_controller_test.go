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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
)

type blockingCleanupScheduler struct {
	*BaseScheduler
	cleanupStarted chan struct{}
	allowCleanup   <-chan struct{}
}

func newBlockingCleanupScheduler(oc *operator.Controller, cleanupStarted chan struct{}, allowCleanup <-chan struct{}) *blockingCleanupScheduler {
	base := NewBaseScheduler(oc, types.EvictLeaderScheduler, nil)
	base.name = "test-blocking-cleanup-scheduler"
	return &blockingCleanupScheduler{
		BaseScheduler:  base,
		cleanupStarted: cleanupStarted,
		allowCleanup:   allowCleanup,
	}
}

func (*blockingCleanupScheduler) Schedule(sche.SchedulerCluster, bool) ([]*operator.Operator, []plan.Plan) {
	return nil, nil
}

func (*blockingCleanupScheduler) IsScheduleAllowed(sche.SchedulerCluster) bool {
	return false
}

func (s *blockingCleanupScheduler) CleanConfig(sche.SchedulerCluster) {
	close(s.cleanupStarted)
	<-s.allowCleanup
}

func TestRemoveSchedulerWaitsForCleanup(t *testing.T) {
	re := require.New(t)
	cleanup, _, tc, oc := prepareSchedulersTest()
	defer cleanup()

	controller := NewController(context.Background(), tc, storage.NewStorageWithMemoryBackend(), oc)
	cleanupStarted := make(chan struct{})
	allowCleanup := make(chan struct{})
	scheduler := newBlockingCleanupScheduler(oc, cleanupStarted, allowCleanup)

	re.NoError(controller.AddScheduler(scheduler))

	removeDone := make(chan error, 1)
	go func() {
		removeDone <- controller.RemoveScheduler(scheduler.GetName())
	}()

	select {
	case <-cleanupStarted:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start")
	}

	select {
	case err := <-removeDone:
		re.FailNow("remove returned before cleanup finished", "err=%v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(allowCleanup)

	select {
	case err := <-removeDone:
		re.NoError(err)
	case <-time.After(time.Second):
		t.Fatal("remove did not finish after cleanup was released")
	}

	controller.Wait()
}
