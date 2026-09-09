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

package scheduling

import (
	"fmt"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

// TestStoreLimitScheduleConfigSync exercises real scheduler selection after the
// PD configuration API, including the etcd watch in standalone scheduling.
func TestStoreLimitScheduleConfigSync(t *testing.T) {
	defaults := sc.DefaultStoreLimitConfig()
	t.Cleanup(func() {
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, defaults.AddPeer)
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, defaults.RemovePeer)
	})
	for _, deployment := range []tests.Env{tests.NonMicroserviceEnv, tests.MicroserviceEnv} {
		t.Run(fmt.Sprint(deployment), func(t *testing.T) {
			re := require.New(t)
			env := tests.NewSchedulingTestEnvironment(t, func(cfg *config.Config, _ string) {
				// Call Schedule explicitly so background operators cannot refresh limiters
				// or consume the test's token budgets before selection is observed.
				cfg.Schedule.HaltScheduling = true
				cfg.Replication.EnablePlacementRules = false
				cfg.Schedule.DefaultStoreLimit.AddPeer = storelimit.Unlimited
				cfg.Schedule.DefaultStoreLimit.RemovePeer = storelimit.Unlimited
			})
			env.Env = deployment
			defer env.Cleanup()
			env.RunTest(func(tc *tests.TestCluster) {
				pd := tc.GetLeaderServer().GetServer()
				rc := pd.GetRaftCluster()
				var cluster sche.SchedulerCluster = rc
				if service := tc.GetSchedulingPrimaryServer(); service != nil {
					cluster = service.GetCluster()
				}
				testutil.Eventually(re, func() bool { return cluster.IsSchedulingHalted() })
				bc := cluster.GetBasicCluster()
				for _, typ := range []storelimit.Type{storelimit.AddPeer, storelimit.RemovePeer} {
					t.Run(typ.String(), func(t *testing.T) {
						re := require.New(t)
						// The two deployments must own separate limiter objects. Sharing a
						// StoreInfo clone here would let PD's eager refresh hide the MCS bug.
						for _, cache := range []*core.BasicCluster{rc.GetBasicCluster(), bc} {
							for _, store := range cache.GetStores() {
								cache.DeleteStore(store)
							}
							for id := uint64(11); id <= 14; id++ {
								store := core.NewStoreInfo(&metapb.Store{Id: id},
									core.SetLastHeartbeatTS(time.Now()),
									core.SetStoreStats(&pdpb.StoreStats{Capacity: 100 * units.GiB, Available: 80 * units.GiB, UsedSize: 20 * units.GiB}))
								if typ == storelimit.RemovePeer && (id == 12 || id == 13) {
									store = store.Clone(core.SetRegionWeight(1000))
								}
								cache.PutStore(store)
							}
						}
						peers := []*metapb.Peer{{Id: 111, StoreId: 11}, {Id: 112, StoreId: 12}, {Id: 113, StoreId: 13}}
						for i := range 100 {
							bc.PutRegion(core.NewRegionInfo(&metapb.Region{Id: uint64(100 + i), StartKey: []byte(fmt.Sprintf("%04d", i)), EndKey: []byte(fmt.Sprintf("%04d", i+1)), Peers: peers, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, peers[0], core.SetApproximateSize(96)))
						}
						bc.UpdateAllStoreStatus()
						schedulerType := types.BalanceRegionScheduler
						limitedStore := uint64(14)
						if typ == storelimit.RemovePeer {
							limitedStore = 11
						}
						// Initialize the exhausted-low-rate reproduction before any config
						// updates; later updates must recover through Schedule alone.
						bc.ResetStoreLimit(limitedStore, typ, 0.0001/60)
						oc := operator.NewController(t.Context(), bc, cluster.GetSharedConfig(), nil)
						scheduler, err := schedulers.CreateScheduler(schedulerType, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigSliceDecoder(schedulerType, []string{"", ""}))
						re.NoError(err)
						setRate := func(rate float64, all bool) {
							url := fmt.Sprintf("%s/pd/api/v1/store/%d/limit", pd.GetAddr(), limitedStore)
							if all {
								url = fmt.Sprintf("%s/pd/api/v1/stores/limit", pd.GetAddr())
							}
							body := fmt.Sprintf(`{"type":%q,"rate":%g}`, typ.String(), rate)
							re.NoError(testutil.CheckPostJSON(tests.TestDialClient, url, []byte(body), testutil.StatusOK(re)))
							// Observe only configuration here: no controller or limiter refresh.
							testutil.Eventually(re, func() bool {
								return cluster.GetSharedConfig().GetStoreLimitByType(limitedStore, typ) == rate
							})
						}
						schedule := func() []*operator.Operator {
							ops, _ := scheduler.Schedule(cluster, false)
							return ops
						}
						for i, rate := range []float64{0.0001, 30, 0.0001, storelimit.Unlimited} {
							setRate(rate, i%2 == 1)
							// This must be the first budget check after configuration arrives.
							ops := schedule()
							re.NotEmpty(ops, "rate %g", rate)
							influence := operator.NewTotalOpInfluence(ops, bc)
							re.Positive(influence.GetStoreInfluence(limitedStore).GetStepCost(typ))
							limiter := bc.GetStore(limitedStore).GetStoreLimit()
							re.Equal(rate/60, limiter.(*storelimit.StoreRateLimit).Rate(typ))
							if rate == storelimit.Unlimited {
								re.True(limiter.Take(100*storelimit.RegionInfluence[typ], typ, constant.Medium))
								re.NotEmpty(schedule())
								continue
							}
							re.True(limiter.Take(storelimit.RegionInfluence[typ], typ, constant.Medium))
							re.Empty(schedule(), "exhausted rate %g", rate)
						}
					})
				}
			})
		})
	}
}
