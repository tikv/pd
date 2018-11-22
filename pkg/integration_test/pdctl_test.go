// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/tools/pd-ctl/pdctl"
	"github.com/pingcap/pd/tools/pd-ctl/pdctl/command"
	"github.com/spf13/cobra"

	// Register schedulers.
	_ "github.com/pingcap/pd/server/schedulers"
)

func (s *integrationTestSuite) TestCluster(c *C) {
	c.Parallel()

	cluster, err := newTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.config.GetClientURLs()
	cmd := initCommand()
	defer cluster.Destroy()

	// cluster command
	args := []string{"-u", pdAddr, "cluster"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	ci := &metapb.Cluster{}
	c.Assert(json.Unmarshal(output, ci), IsNil)
	c.Assert(ci, DeepEquals, cluster.GetCluster())

	args = []string{"-u", pdAddr, "cluster", "status"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	ci = &metapb.Cluster{}
	c.Assert(json.Unmarshal(output, ci), IsNil)
	c.Assert(ci, DeepEquals, cluster.GetCluster())
}

func (s *integrationTestSuite) TestHealth(c *C) {
	c.Parallel()

	cluster, err := newTestCluster(3)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.config.GetClientURLs()
	cmd := initCommand()
	defer cluster.Destroy()

	client := cluster.GetEtcdClient()
	members, err := server.GetMembers(client)
	c.Assert(err, IsNil)
	unhealthMembers := cluster.CheckHealth(members)
	healths := []api.Health{}
	for _, member := range members {
		h := api.Health{
			Name:       member.Name,
			MemberID:   member.MemberId,
			ClientUrls: member.ClientUrls,
			Health:     true,
		}
		if _, ok := unhealthMembers[member.GetMemberId()]; ok {
			h.Health = false
		}
		healths = append(healths, h)
	}

	// health command
	args := []string{"-u", pdAddr, "health"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	h := make([]api.Health, len(healths))
	c.Assert(json.Unmarshal(output, &h), IsNil)
	c.Assert(err, IsNil)
	c.Assert(h, DeepEquals, healths)
}

func (s *integrationTestSuite) TestStore(c *C) {
	c.Parallel()

	cluster, err := newTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.config.GetClientURLs()
	cmd := initCommand()

	stores := []*metapb.Store{
		{
			Id:      1,
			Address: "tikv1",
			State:   metapb.StoreState_Up,
			Version: "2.0.0",
		},
		{
			Id:      2,
			Address: "tikv3",
			State:   metapb.StoreState_Tombstone,
			Version: "2.0.0",
		},
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	s.bootstrapCluster(leaderServer, c)

	for _, store := range stores {
		mustPutStore(c, leaderServer.server, store.Id, store.State, store.Labels)
	}
	defer cluster.Destroy()

	// store command
	args := []string{"-u", pdAddr, "store"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	storesInfo := new(api.StoresInfo)
	c.Assert(json.Unmarshal(output, &storesInfo), IsNil)
	checkStoresInfo(c, storesInfo.Stores, stores[:1])

	// store <store_id> command
	args = []string{"-u", pdAddr, "store", "1"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	storeInfo := new(api.StoreInfo)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	checkStoresInfo(c, []*api.StoreInfo{storeInfo}, stores[:1])

	// store label <store_id> <key> <value> command
	c.Assert(storeInfo.Store.Labels, IsNil)
	args = []string{"-u", pdAddr, "store", "label", "1", "zone", "cn"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	label := storeInfo.Store.Labels[0]
	c.Assert(label.Key, Equals, "zone")
	c.Assert(label.Value, Equals, "cn")

	// store weight <store_id> <leader_weight> <region_weight> command
	c.Assert(storeInfo.Status.LeaderWeight, Equals, float64(1))
	c.Assert(storeInfo.Status.RegionWeight, Equals, float64(1))
	args = []string{"-u", pdAddr, "store", "weight", "1", "5", "10"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	c.Assert(storeInfo.Status.LeaderWeight, Equals, float64(5))
	c.Assert(storeInfo.Status.RegionWeight, Equals, float64(10))

	// store delete <store_id> command
	c.Assert(storeInfo.Store.State, Equals, metapb.StoreState_Up)
	args = []string{"-u", pdAddr, "store", "delete", "1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	c.Assert(storeInfo.Store.State, Equals, metapb.StoreState_Offline)
}

func (s *integrationTestSuite) TestLabel(c *C) {
	c.Parallel()

	cluster, err := newTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.config.GetClientURLs()
	cmd := initCommand()

	stores := []*metapb.Store{
		{
			Id:      1,
			Address: "tikv1",
			State:   metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-west",
				},
			},
			Version: "2.0.0",
		},
		{
			Id:      2,
			Address: "tikv2",
			State:   metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-east",
				},
			},
			Version: "2.0.0",
		},
		{
			Id:      3,
			Address: "tikv3",
			State:   metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-west",
				},
			},
			Version: "2.0.0",
		},
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	s.bootstrapCluster(leaderServer, c)

	for _, store := range stores {
		mustPutStore(c, leaderServer.server, store.Id, store.State, store.Labels)
	}
	defer cluster.Destroy()

	// label command
	args := []string{"-u", pdAddr, "label"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	labels := make([]*metapb.StoreLabel, 0, len(stores))
	c.Assert(json.Unmarshal(output, &labels), IsNil)
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
	c.Assert(got, DeepEquals, expected)

	// label store <name> command
	args = []string{"-u", pdAddr, "label", "store", "zone", "us-west"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	storesInfo := new(api.StoresInfo)
	c.Assert(json.Unmarshal(output, &storesInfo), IsNil)
	ss = []*metapb.Store{stores[0], stores[2]}
	checkStoresInfo(c, storesInfo.Stores, ss)
}

func (s *integrationTestSuite) TestTSO(c *C) {
	c.Parallel()
	cmd := initCommand()

	const (
		physicalShiftBits = 18
		logicalBits       = 0x3FFFF
	)

	// tso command
	ts := "395181938313123110"
	args := []string{"-u", "127.0.0.1", "tso", ts}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	t, e := strconv.ParseUint(ts, 10, 64)
	c.Assert(e, IsNil)
	c.Assert(err, IsNil)
	logicalTime := t & logicalBits
	physical := t >> physicalShiftBits
	physicalTime := time.Unix(int64(physical/1000), int64(physical%1000)*time.Millisecond.Nanoseconds())
	str := fmt.Sprintln("system: ", physicalTime) + fmt.Sprintln("logic: ", logicalTime)
	c.Assert(str, Equals, string(output))
}

func (s *integrationTestSuite) TestScheduler(c *C) {
	c.Parallel()

	cluster, err := newTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.config.GetClientURLs()
	cmd := initCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	s.bootstrapCluster(leaderServer, c)
	mustPutStore(c, leaderServer.server, store.Id, store.State, store.Labels)

	region := core.NewRegionInfo(&metapb.Region{
		Id:       1,
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
		Peers: []*metapb.Peer{
			{Id: 2, StoreId: 1},
		},
	},
		&metapb.Peer{Id: 2, StoreId: 1},
	)
	err = cluster.HandleRegionHeartbeat(region)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	time.Sleep(3 * time.Second)
	// scheduler show command
	args := []string{"-u", pdAddr, "scheduler", "show"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	var schedulers []string
	c.Assert(json.Unmarshal(output, &schedulers), IsNil)
	expected := map[string]bool{
		"balance-region-scheduler":     true,
		"balance-leader-scheduler":     true,
		"balance-hot-region-scheduler": true,
		"label-scheduler":              true,
	}
	for _, scheduler := range schedulers {
		c.Assert(expected[scheduler], Equals, true)
	}

	// scheduler add command
	args = []string{"-u", pdAddr, "scheduler", "add", "grant-leader-scheduler", "1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "scheduler", "show"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	schedulers = schedulers[:0]
	c.Assert(json.Unmarshal(output, &schedulers), IsNil)
	expected = map[string]bool{
		"balance-region-scheduler":     true,
		"balance-leader-scheduler":     true,
		"balance-hot-region-scheduler": true,
		"label-scheduler":              true,
		"grant-leader-scheduler-1":     true,
	}
	for _, scheduler := range schedulers {
		c.Assert(expected[scheduler], Equals, true)
	}

	// scheduler delete command
	args = []string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "scheduler", "show"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	schedulers = schedulers[:0]
	c.Assert(json.Unmarshal(output, &schedulers), IsNil)
	expected = map[string]bool{
		"balance-leader-scheduler":     true,
		"balance-hot-region-scheduler": true,
		"label-scheduler":              true,
		"grant-leader-scheduler-1":     true,
	}
	for _, scheduler := range schedulers {
		c.Assert(expected[scheduler], Equals, true)
	}
}

func initCommand() *cobra.Command {
	commandFlags := pdctl.CommandFlags{}
	rootCmd := &cobra.Command{}
	rootCmd.PersistentFlags().StringVarP(&commandFlags.URL, "pd", "u", "", "")
	rootCmd.Flags().StringVar(&commandFlags.CAPath, "cacert", "", "")
	rootCmd.Flags().StringVar(&commandFlags.CertPath, "cert", "", "")
	rootCmd.Flags().StringVar(&commandFlags.KeyPath, "key", "", "")
	rootCmd.AddCommand(
		command.NewConfigCommand(),
		command.NewRegionCommand(),
		command.NewStoreCommand(),
		command.NewMemberCommand(),
		command.NewExitCommand(),
		command.NewLabelCommand(),
		command.NewPingCommand(),
		command.NewOperatorCommand(),
		command.NewSchedulerCommand(),
		command.NewTSOCommand(),
		command.NewHotSpotCommand(),
		command.NewClusterCommand(),
		command.NewTableNamespaceCommand(),
		command.NewHealthCommand(),
		command.NewLogCommand(),
	)
	return rootCmd
}

func executeCommandC(root *cobra.Command, args ...string) (c *cobra.Command, output []byte, err error) {
	buf := new(bytes.Buffer)
	root.SetOutput(buf)
	root.SetArgs(args)

	c, err = root.ExecuteC()
	return c, buf.Bytes(), err
}

func checkStoresInfo(c *C, ss []*api.StoreInfo, want []*metapb.Store) {
	c.Assert(len(ss), Equals, len(want))
	mapWant := make(map[uint64]*metapb.Store)
	for _, s := range want {
		if _, ok := mapWant[s.Id]; !ok {
			mapWant[s.Id] = s
		}
	}
	for _, s := range ss {
		c.Assert(s.Store.Store, DeepEquals, mapWant[s.Store.Store.Id])
	}
}

func mustPutStore(c *C, svr *server.Server, id uint64, state metapb.StoreState, labels []*metapb.StoreLabel) {
	_, err := svr.PutStore(context.Background(), &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: svr.ClusterID()},
		Store: &metapb.Store{
			Id:      id,
			Address: fmt.Sprintf("tikv%d", id),
			State:   state,
			Labels:  labels,
			Version: server.MinSupportedVersion(server.Version2_0).String(),
		},
	})
	c.Assert(err, IsNil)
}
