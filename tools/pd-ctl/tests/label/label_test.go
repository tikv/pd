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

package label_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/response"
	"github.com/tikv/pd/server/config"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

func TestLabel(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := pdTests.NewTestCluster(ctx, 1, func(cfg *config.Config, _ string) { cfg.Replication.StrictlyMatchLabel = false })
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	stores := []*response.StoreInfo{
		{
			Store: &response.MetaStore{
				Store: &metapb.Store{
					Id:    1,
					State: metapb.StoreState_Up,
					Labels: []*metapb.StoreLabel{
						{
							Key:   "zone",
							Value: "us-west",
						},
					},
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Up.String(),
			},
		},
		{
			Store: &response.MetaStore{
				Store: &metapb.Store{
					Id:    2,
					State: metapb.StoreState_Up,
					Labels: []*metapb.StoreLabel{
						{
							Key:   "zone",
							Value: "us-east",
						},
					},
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Up.String(),
			},
		},
		{
			Store: &response.MetaStore{
				Store: &metapb.Store{
					Id:    3,
					State: metapb.StoreState_Up,
					Labels: []*metapb.StoreLabel{
						{
							Key:   "zone",
							Value: "us-west",
						},
					},
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Up.String(),
			},
		},
	}
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	for _, store := range stores {
		pdTests.MustPutStore(re, cluster, store.Store.Store)
	}
	defer cluster.Destroy()

	// label command
	args := []string{"-u", pdAddr, "label"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	labels := make([]*metapb.StoreLabel, 0, len(stores))
	re.NoError(json.Unmarshal(output, &labels))
	got := make(map[string]struct{})
	for _, l := range labels {
		if _, ok := got[strings.ToLower(l.Key+l.Value)]; !ok {
			got[strings.ToLower(l.Key+l.Value)] = struct{}{}
		}
	}
	expected := make(map[string]struct{})
	ss := leaderServer.GetStores()
	for _, s := range ss {
		ls := s.GetLabels()
		for _, l := range ls {
			if _, ok := expected[strings.ToLower(l.Key+l.Value)]; !ok {
				expected[strings.ToLower(l.Key+l.Value)] = struct{}{}
			}
		}
	}
	re.Equal(expected, got)

	// label store <name> command
	args = []string{"-u", pdAddr, "label", "store", "zone", "us-west"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storesInfo := new(response.StoresInfo)
	re.NoError(json.Unmarshal(output, &storesInfo))
	sss := []*response.StoreInfo{stores[0], stores[2]}
	tests.CheckStoresInfo(re, storesInfo.Stores, sss)

	// label isolation [label]
	args = []string{"-u", pdAddr, "label", "isolation"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "none")
	re.Contains(string(output), "2")
}
