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

package filter_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	mcsconfig "github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/filter"
)

func TestStoreLimitConfigSync(t *testing.T) {
	defer goleak.VerifyNone(t)
	for _, deployment := range []string{"PD", "MCS"} {
		for _, version := range []string{storelimit.VersionV1, storelimit.VersionV2} {
			for _, typ := range []storelimit.Type{storelimit.AddPeer, storelimit.RemovePeer} {
				t.Run(deployment+"/"+version+"/"+typ.String(), func(t *testing.T) {
					re := require.New(t)
					opt := mockconfig.NewTestOptions()
					var conf config.SchedulerConfigProvider = opt
					if deployment == "MCS" {
						conf = mcsconfig.NewPersistConfig(&mcsconfig.Config{Schedule: *opt.GetScheduleConfig(), Replication: *opt.GetReplicationConfig()}, nil)
					}
					limit := storelimit.NewStoreRateLimit(0.0001 / 60)
					if version == storelimit.VersionV2 {
						limit = storelimit.NewSlidingWindows()
					}
					store := core.NewStoreInfo(&metapb.Store{Id: 123}, core.SetLastHeartbeatTS(time.Now()), core.SetStoreLimit(limit))
					f := &filter.StoreStateFilter{MoveRegion: true, OperatorLevel: constant.Medium}
					available := func() bool {
						if typ == storelimit.RemovePeer {
							return f.Source(conf, store).IsOK()
						}
						return f.Target(conf, store).IsOK()
					}
					setRate := func(rate float64) {
						next := conf.GetScheduleConfig().Clone()
						// Missing store entries must use the current default without copying or
						// mutating configuration on the read path.
						next.DefaultStoreLimit = config.StoreLimitConfig{AddPeer: rate, RemovePeer: rate}
						conf.SetScheduleConfig(next)
					}
					cost := storelimit.RegionInfluence[typ]
					for _, rate := range []float64{0.0001, 30, 0.0001, storelimit.Unlimited, 0, 0.0001} {
						setRate(rate)
						snapshot := conf.GetScheduleConfig()
						re.True(available(), "new rate %g must apply during selection", rate)
						re.Same(snapshot, conf.GetScheduleConfig())
						re.NotContains(snapshot.StoreLimit, store.GetID())
						if version == storelimit.VersionV2 {
							continue
						}
						re.Equal(rate/60, limit.(*storelimit.StoreRateLimit).Rate(typ))
						// Unlimited and zero retain their existing semantics.
						if rate == storelimit.Unlimited || rate == 0 {
							re.True(limit.Take(cost*100, typ, constant.Medium))
							re.True(available())
							continue
						}
						re.True(limit.Take(cost, typ, constant.Medium))
						re.False(available())
						// Reinstalling the same config must not refill the exhausted bucket.
						setRate(rate)
						re.False(available())
					}
				})
			}
		}
	}
}

func TestStoreLimitConcurrentConfigSync(t *testing.T) {
	defer goleak.VerifyNone(t)
	re := require.New(t)
	conf := mockconfig.NewTestOptions()
	low := conf.GetScheduleConfig().Clone()
	low.DefaultStoreLimit.AddPeer = 0.0001
	high := low.Clone()
	high.DefaultStoreLimit.AddPeer = 30
	conf.SetScheduleConfig(low)
	store := core.NewStoreInfo(&metapb.Store{Id: 123}, core.SetLastHeartbeatTS(time.Now()))
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			f := &filter.StoreStateFilter{MoveRegion: true}
			for range 1000 {
				f.Target(conf, store)
				store.GetStoreLimit().Take(1000, storelimit.AddPeer, constant.Medium)
			}
		})
	}
	for range 1000 {
		conf.SetScheduleConfig(high)
		conf.SetScheduleConfig(low)
	}
	wg.Wait()
	conf.SetScheduleConfig(high)
	f := &filter.StoreStateFilter{MoveRegion: true}
	re.True(f.Target(conf, store).IsOK())
	re.Equal(0.5, store.GetStoreLimit().(*storelimit.StoreRateLimit).Rate(storelimit.AddPeer))
	re.Empty(conf.GetScheduleConfig().StoreLimit)
}
