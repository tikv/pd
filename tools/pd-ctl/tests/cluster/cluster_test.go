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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	clusterpkg "github.com/tikv/pd/server/cluster"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

func TestClusterAndPing(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := pdTests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	err = cluster.GetLeaderServer().BootstrapCluster()
	re.NoError(err)
	pdAddr := cluster.GetConfig().GetClientURL()
	i := strings.Index(pdAddr, "//")
	pdAddr = pdAddr[i+2:]
	cmd := ctl.GetRootCmd()

	// cluster
	args := []string{"-u", pdAddr, "cluster"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	ci := &metapb.Cluster{}
	re.NoError(json.Unmarshal(output, ci))
	re.Equal(cluster.GetCluster(), ci)

	// cluster info
	args = []string{"-u", pdAddr, "cluster"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	ci = &metapb.Cluster{}
	re.NoError(json.Unmarshal(output, ci))
	re.Equal(cluster.GetCluster(), ci)

	// cluster status
	args = []string{"-u", pdAddr, "cluster", "status"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	cs := &clusterpkg.Status{}
	re.NoError(json.Unmarshal(output, cs))
	clusterStatus, err := cluster.GetClusterStatus()
	re.NoError(err)
	re.True(clusterStatus.RaftBootstrapTime.Equal(cs.RaftBootstrapTime))
	// ref: https://github.com/onsi/gomega/issues/264
	clusterStatus.RaftBootstrapTime = time.Time{}
	cs.RaftBootstrapTime = time.Time{}

	re.Equal(clusterStatus, cs)

	// ping
	args = []string{"-u", pdAddr, "ping"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NotNil(output)

	// does not exist
	args = []string{"-u", pdAddr, "--cacert=ca.pem", "cluster"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.Contains(err.Error(), "no such file or directory")
}
