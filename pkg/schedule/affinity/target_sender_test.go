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

package affinity

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/storage"
)

type targetWriterFunc func(context.Context, []GroupTarget) ([]string, error)

func (f targetWriterFunc) WriteGroupTargets(ctx context.Context, targets []GroupTarget) ([]string, error) {
	return f(ctx, targets)
}

func TestFirstWinSingleFlightAndGlobalBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, _ := newTargetTestManager(t)
		re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))
		calls := 0
		m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
			calls++
			if calls == 1 {
				time.Sleep(3 * time.Second)
				return nil, errors.New("PD unavailable")
			}
			m.SyncGroupFromEtcd(targets[0].result())
			return []string{targets[0].Expected.ID}, nil
		}))
		region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
		observe := func(id string) { m.ObserveAvailableRegion(region, m.GetAffinityGroupState(id)) }
		observe("g")
		synctest.Wait()
		for range 10 {
			observe("g")
			observe("h")
		}
		synctest.Wait()
		re.Equal(1, calls, "other groups and repeated Regions are discarded while busy")
		time.Sleep(3 * time.Second)
		synctest.Wait()
		observe("h")
		synctest.Wait()
		re.Equal(1, calls, "failure delays all groups")
		time.Sleep(time.Second)
		observe("h")
		synctest.Wait()
		re.Equal(2, calls)
		re.True(m.GetAffinityGroupState("h").AffinitySchedulingAllowed)
		re.Zero(m.GetAffinityGroupState("g").LeaderStoreID, "discarded observations are never replayed")
		observe("g")
		synctest.Wait()
		re.Equal(3, calls, "success has no global cooldown")
		re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	})
}

func TestFirstWinCancelsInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		m := newTargetTestManagerWith(t, ctx, storage.NewStorageWithMemoryBackend(), time.Now())
		re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
		calls := 0
		stopped := false
		m.SetTargetWriter(targetWriterFunc(func(ctx context.Context, _ []GroupTarget) ([]string, error) {
			calls++
			<-ctx.Done()
			stopped = true
			return nil, ctx.Err()
		}))
		region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
		m.ObserveAvailableRegion(region, m.GetAffinityGroupState("g"))
		synctest.Wait()
		re.Equal(1, calls)
		cancel()
		synctest.Wait()
		re.True(stopped)
		m.ObserveAvailableRegion(region, m.GetAffinityGroupState("g"))
		synctest.Wait()
		re.Equal(1, calls)
	})
}

func TestFirstWinReplyKeepsNewerWatcherTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, _ := newTargetTestManager(t)
		re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
		newer := m.GetAffinityGroupState("g").Group
		newer.LeaderStoreID, newer.VoterStoreIDs = 2, []uint64{1, 2}
		m.SetTargetWriter(targetWriterFunc(func(_ context.Context, _ []GroupTarget) ([]string, error) {
			// An admin write reaches the watcher before the old HTTP response.
			m.SyncGroupFromEtcd(&newer)
			return []string{"g"}, nil
		}))
		m.ObserveAvailableRegion(generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange), m.GetAffinityGroupState("g"))
		synctest.Wait()
		re.Equal(newer, m.GetAffinityGroupState("g").Group)
	})
}
