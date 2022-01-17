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

package cluster_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server"
	clusterpkg "github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&clusterTestSuite{})

type clusterTestSuite struct{}

func (s *clusterTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *clusterTestSuite) TestClusterAndPing(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	err = cluster.GetServer(cluster.GetLeader()).BootstrapCluster()
	c.Assert(err, IsNil)
	pdAddr := cluster.GetConfig().GetClientURL()
	i := strings.Index(pdAddr, "//")
	pdAddr = pdAddr[i+2:]
	cmd := pdctlCmd.GetRootCmd()
	defer cluster.Destroy()

	// cluster
	args := []string{"-u", pdAddr, "cluster"}
	output, err := pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	ci := &metapb.Cluster{}
	fmt.Println("_______", string(output))
	c.Assert(json.Unmarshal(output, ci), IsNil)
	c.Assert(ci, DeepEquals, cluster.GetCluster())

	// cluster info
	args = []string{"-u", pdAddr, "cluster"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	ci = &metapb.Cluster{}
	c.Assert(json.Unmarshal(output, ci), IsNil)
	c.Assert(ci, DeepEquals, cluster.GetCluster())

	// cluster status
	args = []string{"-u", pdAddr, "cluster", "status"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	cs := &clusterpkg.Status{}
	c.Assert(json.Unmarshal(output, cs), IsNil)
	clusterStatus, err := cluster.GetClusterStatus()
	c.Assert(err, IsNil)
	c.Assert(clusterStatus.RaftBootstrapTime.Equal(cs.RaftBootstrapTime), IsTrue)
	// ref: https://github.com/onsi/gomega/issues/264
	clusterStatus.RaftBootstrapTime = time.Time{}
	cs.RaftBootstrapTime = time.Time{}

	c.Assert(cs, DeepEquals, clusterStatus)

	// ping
	args = []string{"-u", pdAddr, "ping"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(output, NotNil)

	// does not exist
	args = []string{"-u", pdAddr, "--cacert=ca.pem", "cluster"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, ErrorMatches, ".*no such file or directory.*")
}
