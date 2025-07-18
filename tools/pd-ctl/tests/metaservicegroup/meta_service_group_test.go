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

package metaservicegroup_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

func mockMetaServiceGroups() map[string]string {
	return map[string]string{
		"etcd-group-0": "etcd-group-0.tidb-serverless.cluster.svc.local",
		"etcd-group-1": "etcd-group-1.tidb-serverless.cluster.svc.local",
	}
}

func findGroup(groups []*handlers.MetaServiceGroupStatus, id string) *handlers.MetaServiceGroupStatus {
	for _, group := range groups {
		if group.ID == id {
			return group
		}
	}
	return nil
}

func TestMetaServiceGroupCommands(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := pdTests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Keyspace.WaitRegionSplit = false
		conf.Keyspace.MetaServiceGroups = mockMetaServiceGroups()
	})
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	// list should return the pre-configured groups, all disabled by default.
	var groups []*handlers.MetaServiceGroupStatus
	tests.MustExec(re, cmd, []string{"-u", pdAddr, "meta-service-group", "list"}, &groups)
	re.Len(groups, len(mockMetaServiceGroups()))
	for _, group := range groups {
		re.Contains(mockMetaServiceGroups(), group.ID)
		re.NotNil(group.Status)
		re.False(group.Status.Enabled)
		re.Zero(group.Status.AssignmentCount)
	}

	// upsert a new meta-service group.
	cmd = ctl.GetRootCmd()
	tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "meta-service-group", "upsert",
		"-g", "etcd-group-2=etcd-group-2.tidb-serverless.cluster.svc.local",
	}, nil)
	cmd = ctl.GetRootCmd()
	tests.MustExec(re, cmd, []string{"-u", pdAddr, "meta-service-group", "list"}, &groups)
	re.Len(groups, len(mockMetaServiceGroups())+1)
	added := findGroup(groups, "etcd-group-2")
	re.NotNil(added)
	re.Equal("etcd-group-2.tidb-serverless.cluster.svc.local", added.Addresses)
	re.False(added.Status.Enabled)

	// set-enabled should flip the enabled state.
	cmd = ctl.GetRootCmd()
	tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "meta-service-group", "set-enabled", "etcd-group-0", "true",
	}, &groups)
	re.True(findGroup(groups, "etcd-group-0").Status.Enabled)
	re.False(findGroup(groups, "etcd-group-1").Status.Enabled)

	// set-assignment-count should update the assignment count.
	cmd = ctl.GetRootCmd()
	tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "meta-service-group", "set-assignment-count", "etcd-group-0", "10",
	}, &groups)
	re.Equal(10, findGroup(groups, "etcd-group-0").Status.AssignmentCount)
	// enabled state should be preserved across an assignment-count patch.
	re.True(findGroup(groups, "etcd-group-0").Status.Enabled)

	// set-enabled false should disable the group again.
	cmd = ctl.GetRootCmd()
	tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "meta-service-group", "set-enabled", "etcd-group-0", "false",
	}, &groups)
	re.False(findGroup(groups, "etcd-group-0").Status.Enabled)
	// assignment count should be preserved across an enabled patch.
	re.Equal(10, findGroup(groups, "etcd-group-0").Status.AssignmentCount)

	// invalid boolean for set-enabled should report an error and not change state.
	cmd = ctl.GetRootCmd()
	out := tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "meta-service-group", "set-enabled", "etcd-group-0", "notabool",
	}, nil)
	re.Contains(out, "Invalid value for enabled flag")

	// invalid integer for set-assignment-count should report an error.
	cmd = ctl.GetRootCmd()
	out = tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "meta-service-group", "set-assignment-count", "etcd-group-0", "notanint",
	}, nil)
	re.Contains(out, "Invalid value for assignment count")

	// negative assignment count should be rejected by the server (HTTP 400).
	cmd = ctl.GetRootCmd()
	out = tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "meta-service-group", "set-assignment-count", "--", "etcd-group-0", "-1",
	}, nil)
	re.Contains(out, "assignment count must be non-negative")

	// patching an unknown group should be rejected by the server (HTTP 404).
	cmd = ctl.GetRootCmd()
	out = tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "meta-service-group", "set-enabled", "nonexistent-group", "true",
	}, nil)
	re.Contains(out, "unknown meta-service group")
}
