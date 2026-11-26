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

package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockid"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/server/config"
)

type storeLimitRefreshReadHook struct {
	*config.PersistOptions
	beforeRead func()
}

func (c *storeLimitRefreshReadHook) GetStoreLimitByType(storeID uint64, typ storelimit.Type) float64 {
	c.beforeRead()
	return c.PersistOptions.GetStoreLimitByType(storeID, typ)
}

func (c *storeLimitRefreshReadHook) PeekStoreLimitByType(storeID uint64, typ storelimit.Type) float64 {
	c.beforeRead()
	return c.PersistOptions.PeekStoreLimitByType(storeID, typ)
}

func TestStoreLimitRefreshDoesNotWaitForPersistence(t *testing.T) {
	for _, typ := range []storelimit.Type{storelimit.AddPeer, storelimit.RemovePeer, storelimit.TransferLeaderIn} {
		t.Run(typ.String(), func(t *testing.T) {
			re := require.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			wg := &sync.WaitGroup{}
			defer func() {
				cancel()
				wg.Wait()
			}()

			_, opt, err := newTestScheduleConfig()
			re.NoError(err)
			cfg := opt.GetScheduleConfig().Clone()
			cfg.DefaultStoreLimit = sc.StoreLimitConfig{AddPeer: 120, RemovePeer: 180, TransferLeaderIn: 240}
			cfg.StoreLimit[1] = sc.StoreLimitConfig{AddPeer: 0.00006, RemovePeer: 0.00006, TransferLeaderIn: 0.00006}
			opt.SetScheduleConfig(cfg)
			backend := storage.NewStorageWithMemoryBackend()
			rc := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, backend)
			rc.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1}))
			peers := []*metapb.Peer{{Id: 2, StoreId: 2}}
			var step operator.OpStep = operator.AddPeer{ToStore: 1, PeerID: 1}
			if typ != storelimit.AddPeer {
				peers = append(peers, &metapb.Peer{Id: 1, StoreId: 1})
				step = operator.RemovePeer{FromStore: 1, PeerID: 1}
				if typ == storelimit.TransferLeaderIn {
					step = operator.TransferLeader{FromStore: 2, ToStore: 1}
				}
			}
			region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[0], core.SetApproximateSize(96))
			rc.PutRegion(region)

			// Pause the controller's second config read, which runs while
			// Refresh holds the limiter lock. Hook both getters so switching
			// back to the mutating getter also exercises this interleaving.
			refreshLocked := make(chan struct{})
			resumeRead := make(chan struct{})
			reads := 0
			provider := &storeLimitRefreshReadHook{PersistOptions: opt, beforeRead: func() {
				reads++
				if reads == 2 {
					close(refreshLocked)
					select {
					case <-resumeRead:
					case <-ctx.Done():
					}
				}
			}}
			controller := operator.NewController(ctx, rc.BasicCluster, provider, nil)
			op := operator.NewTestOperator(1, region.GetRegionEpoch(), operator.OpRegion, step)
			admissionDone := make(chan bool, 1)
			wg.Go(func() {
				admissionDone <- controller.ExceedStoreLimit(op)
			})
			select {
			case <-refreshLocked:
			case <-time.After(5 * time.Second):
				re.FailNow("controller did not reach the locked config read")
			}

			// Removal publishes the missing entry before resetting the limiter.
			// Its reset must wait for the controller to release the limiter lock.
			removalDone := make(chan error, 1)
			wg.Go(func() {
				removalDone <- rc.RemoveStoreLimit(1)
			})
			re.Eventually(func() bool {
				_, ok := opt.GetScheduleConfig().StoreLimit[1]
				return !ok
			}, 5*time.Second, time.Millisecond)

			// Pin the persistence transaction that addStoreLimitInternal uses
			// before it reads StoresInfo. Admission must finish without waiting
			// for this transaction, breaking limiter -> persistMu -> StoresInfo
			// -> limiter. Cancellation releases it even if the regression returns.
			persistLocked := make(chan struct{})
			resumePersistence := make(chan struct{})
			persistenceDone := make(chan error, 1)
			wg.Go(func() {
				persistenceDone <- opt.UpdateScheduleConfig(backend, func(_, _ *sc.ScheduleConfig) (bool, error) {
					close(persistLocked)
					select {
					case <-resumePersistence:
					case <-ctx.Done():
					}
					return false, nil
				})
			})
			select {
			case <-persistLocked:
			case <-time.After(5 * time.Second):
				re.FailNow("config transaction did not acquire the persistence lock")
			}
			close(resumeRead)
			select {
			case exceeded := <-admissionDone:
				re.False(exceeded)
			case <-time.After(5 * time.Second):
				re.FailNow("limiter refresh waited for the persistence lock")
			}
			close(resumePersistence)
			re.NoError(<-persistenceDone)
			re.NoError(<-removalDone)
			re.NotContains(opt.GetScheduleConfig().StoreLimit, uint64(1))
		})
	}
}

func TestStoreLimitConcurrentUpdateKeepsLatestRate(t *testing.T) {
	for _, typ := range []storelimit.Type{storelimit.AddPeer, storelimit.RemovePeer, storelimit.TransferLeaderIn} {
		t.Run(typ.String(), func(t *testing.T) {
			re := require.New(t)
			previousDefault := sc.DefaultStoreLimit.GetDefaultStoreLimit(typ)
			t.Cleanup(func() { sc.DefaultStoreLimit.SetDefaultStoreLimit(typ, previousDefault) })
			ctx, cancel := context.WithCancel(t.Context())
			wg := &sync.WaitGroup{}
			defer func() {
				cancel()
				wg.Wait()
			}()

			_, opt, err := newTestScheduleConfig()
			re.NoError(err)
			rc := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
			store := core.NewStoreInfo(&metapb.Store{Id: 1})
			rc.PutStore(store)
			limiter := store.GetStoreLimit().(*storelimit.StoreRateLimit)

			const lowRate = 0.00006
			rateRead := make(chan struct{})
			resumeRefresh := make(chan struct{})
			const fp = "github.com/tikv/pd/server/cluster/storeLimitRateRead"
			re.NoError(failpoint.EnableCall(fp, func(storeID uint64, limitType storelimit.Type, ratePerSec float64) {
				if storeID == 1 && limitType == typ && ratePerSec == lowRate/operator.StoreBalanceBaseTime {
					close(rateRead)
					select {
					case <-resumeRefresh:
					case <-ctx.Done():
					}
				}
			}))
			t.Cleanup(func() { re.NoError(failpoint.Disable(fp)) })

			// Delay the global API's refresh after it captures the low rate.
			globalUpdateDone := make(chan error, 1)
			wg.Go(func() { globalUpdateDone <- rc.SetAllStoresLimit(typ, lowRate) })
			select {
			case <-rateRead:
			case <-time.After(5 * time.Second):
				re.FailNow("global update did not reach the rate read")
			}

			// Apply a newer per-store rate before the delayed refresh resumes.
			re.NoError(rc.SetStoreLimit(1, typ, storelimit.Unlimited))
			re.Equal(storelimit.Unlimited/operator.StoreBalanceBaseTime, limiter.Rate(typ))
			close(resumeRefresh)
			select {
			case err := <-globalUpdateDone:
				re.NoError(err)
			case <-time.After(5 * time.Second):
				re.FailNow("global update did not finish")
			}

			re.Equal(storelimit.Unlimited, opt.PeekStoreLimitByType(1, typ))
			re.Equal(storelimit.Unlimited/operator.StoreBalanceBaseTime, limiter.Rate(typ))
			re.True(limiter.Take(storelimit.RegionInfluence[typ], typ, constant.Medium))
			re.True(rc.GetStore(1).IsAvailable(typ, constant.Medium))
		})
	}
}
