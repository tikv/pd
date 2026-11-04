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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

// TestAffinitySplitRecovery exercises the store-facing PD stream in both modes,
// including forwarding ChangeSplit from scheduling through PD in MCS mode.
func TestAffinitySplitRecovery(t *testing.T) {
	for _, mcs := range []bool{false, true} {
		name := "classic"
		if mcs {
			name = "mcs"
		}
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			configure := func(cfg *config.Config, _ string) {
				cfg.Schedule.AffinityScheduleLimit = 4
				cfg.Schedule.MaxAffinityMergeRegionSize = 10
				cfg.Schedule.Schedulers = nil
			}
			newCluster := tests.NewTestCluster
			if mcs {
				newCluster = tests.NewTestClusterWithKeyspaceGroup
			}
			pd, err := newCluster(t.Context(), 1, configure)
			re.NoError(err)
			t.Cleanup(pd.Destroy)
			re.NoError(pd.RunInitialServers())
			re.NotEmpty(pd.WaitLeader())
			leader := pd.GetLeaderServer()
			re.NoError(leader.BootstrapCluster())
			rc := leader.GetRaftCluster()
			manager := rc.GetAffinityManager()
			streams := rc.GetHeartbeatStreams()
			getRegion := rc.GetRegion
			if mcs {
				scheduling, err := tests.NewTestSchedulingCluster(t.Context(), 1, pd)
				re.NoError(err)
				t.Cleanup(scheduling.Destroy)
				scheduling.WaitForPrimaryServing(re)
				cluster := scheduling.GetPrimaryServer().GetCluster()
				manager = cluster.GetAffinityManager()
				streams = cluster.GetHeartbeatStreams()
				getRegion = cluster.GetRegion
				testutil.Eventually(re, func() bool {
					return cluster.GetSharedConfig().GetAffinityScheduleLimit() == 4 &&
						cluster.GetSharedConfig().GetMaxAffinityMergeRegionSize() == 10
				})
			}

			client, conn := testutil.MustNewGrpcClient(re, leader.GetAddr())
			t.Cleanup(func() { re.NoError(conn.Close()) })
			header := testutil.NewRequestHeader(leader.GetClusterID())
			for id := uint64(1); id <= 3; id++ {
				resp, err := client.PutStore(t.Context(), &pdpb.PutStoreRequest{
					Header: header,
					Store: &metapb.Store{
						Id: id, Address: fmt.Sprintf("mock://tikv-%d", id), Version: "7.0.0",
						NodeState: metapb.NodeState_Serving, LastHeartbeat: time.Now().UnixNano(),
					},
				})
				re.NoError(err)
				re.Nil(resp.GetHeader().GetError())
				hb, err := client.StoreHeartbeat(t.Context(), &pdpb.StoreHeartbeatRequest{
					Header: header, Stats: &pdpb.StoreStats{StoreId: id, Capacity: 100 * units.GiB, Available: 90 * units.GiB},
				})
				re.NoError(err)
				re.Nil(hb.GetHeader().GetError())
			}
			// Keep the range below bootstrap keyspace label boundaries.
			re.NoError(rc.GetAffinityManager().CreateAffinityGroups([]affinity.GroupKeyRanges{{
				GroupID: "split-recovery", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("c")}},
			}}))
			_, err = rc.GetAffinityManager().UpdateAffinityGroupPeers("split-recovery", 1, []uint64{1, 2, 3})
			re.NoError(err)
			testutil.Eventually(re, func() bool {
				g := manager.GetAffinityGroupState("split-recovery")
				return g != nil && g.RangeCount == 1 && !g.RegularSchedulingAllowed
			})

			ctx, cancel := context.WithCancel(t.Context())
			heartbeats, err := client.RegionHeartbeat(ctx)
			re.NoError(err)
			responses := make(chan *pdpb.RegionHeartbeatResponse, 16)
			receiveErrors := make(chan error, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					resp, err := heartbeats.Recv()
					if err != nil {
						receiveErrors <- err
						return
					}
					select {
					case responses <- resp:
					case <-ctx.Done():
						return
					}
				}
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})
			peers := []*metapb.Peer{{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2}, {Id: 13, StoreId: 3}}
			req := &pdpb.RegionHeartbeatRequest{
				Header: header,
				Region: &metapb.Region{
					Id: 100, StartKey: []byte("a"), EndKey: []byte("b"), Peers: peers,
					RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
				},
				Leader: peers[0], ApproximateSize: 5 * units.MiB, ApproximateKeys: 500,
			}
			re.NoError(heartbeats.Send(req))
			testutil.Eventually(re, func() bool {
				region := getRegion(100)
				return region != nil && !manager.AllowSplit(region, pdpb.SplitReason_SIZE)
			})
			// A round trip also ensures asynchronous stream binding has completed.
			testutil.Eventually(re, func() bool {
				streams.SendMsg(getRegion(100), &hbstream.Operation{})
				select {
				case resp := <-responses:
					return resp.GetRegionId() == 100
				case err := <-receiveErrors:
					re.NoError(err)
					return false
				default:
					return false
				}
			})
			split, err := client.AskBatchSplit(t.Context(), &pdpb.AskBatchSplitRequest{
				Header: header, Region: req.GetRegion(), SplitCount: 1, Reason: pdpb.SplitReason_SIZE,
			})
			re.NoError(err)
			re.Contains(split.GetHeader().GetError().GetMessage(), "cannot split affinity region")
			re.Empty(split.GetIds())
			waitChangeSplit := func(enabled bool) {
				t.Helper()
				testutil.Eventually(re, func() bool {
					// Reuse unchanged metadata: recovery must not depend on cache updates.
					re.NoError(heartbeats.Send(req))
					select {
					case resp := <-responses:
						if resp.GetRegionId() != 100 || resp.GetChangeSplit() == nil {
							return false
						}
						re.Equal(enabled, resp.GetChangeSplit().GetAutoSplitEnabled())
						re.Equal(req.GetRegion().GetRegionEpoch(), resp.GetRegionEpoch())
						re.Equal(req.GetLeader(), resp.GetTargetPeer())
						return true
					case err := <-receiveErrors:
						re.NoError(err)
						return false
					default:
						return false
					}
				}, testutil.WithWaitFor(10*time.Second))
			}
			waitChangeSplit(false)
			re.NoError(rc.GetAffinityManager().DeleteAffinityGroups([]string{"split-recovery"}, true))
			testutil.Eventually(re, func() bool { return manager.GetAffinityGroupState("split-recovery") == nil })
			waitChangeSplit(true)
		})
	}
}
