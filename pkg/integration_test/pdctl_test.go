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
	"encoding/json"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
	"github.com/pingcap/pd/tools/pd-ctl/pdctl"
	"github.com/pingcap/pd/tools/pd-ctl/pdctl/command"
	"github.com/spf13/cobra"
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

	args := []string{"-u", pdAddr, "health"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	h := make([]api.Health, len(healths))
	c.Assert(json.Unmarshal(output, &h), IsNil)
	c.Assert(err, IsNil)
	c.Assert(h, DeepEquals, healths)
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
