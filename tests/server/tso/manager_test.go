// Copyright 2020 TiKV Project Authors.
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

//go:build tso_full_test || tso_function_test
// +build tso_full_test tso_function_test

package tso_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestClusterDCLocations will write different dc-locations to each server
// and test whether we can get the whole dc-location config from each server.
func TestClusterDCLocations(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testCase := struct {
		dcLocationNumber int
		dcLocationConfig map[string]string
	}{
		dcLocationNumber: 3,
		dcLocationConfig: map[string]string{
			"pd1": "dc-1",
			"pd2": "dc-1",
			"pd3": "dc-2",
			"pd4": "dc-2",
			"pd5": "dc-3",
			"pd6": "dc-3",
		},
	}
	serverNumber := len(testCase.dcLocationConfig)
	cluster, err := tests.NewTestCluster(ctx, serverNumber, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = testCase.dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())

	cluster.WaitAllLeaders(re, testCase.dcLocationConfig)
	serverNameMap := make(map[uint64]string)
	for _, server := range cluster.GetServers() {
		serverNameMap[server.GetServerID()] = server.GetServer().Name()
		// To speed up the test, we force to do the check
		server.GetTSOAllocatorManager().ClusterDCLocationChecker()
	}
	// Start to check every server's GetClusterDCLocations() result
	for _, server := range cluster.GetServers() {
		obtainedServerNumber := 0
		dcLocationMap := server.GetTSOAllocatorManager().GetClusterDCLocations()
		re.NoError(err)
		re.Len(dcLocationMap, testCase.dcLocationNumber)
		for obtainedDCLocation, info := range dcLocationMap {
			obtainedServerNumber += len(info.ServerIDs)
			for _, serverID := range info.ServerIDs {
				expectedDCLocation, exist := testCase.dcLocationConfig[serverNameMap[serverID]]
				re.True(exist)
				re.Equal(expectedDCLocation, obtainedDCLocation)
			}
		}
		re.Equal(serverNumber, obtainedServerNumber)
	}
}

func TestLocalTSOSuffix(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testCase := struct {
		dcLocations      []string
		dcLocationConfig map[string]string
	}{
		dcLocations: []string{"dc-1", "dc-2", "dc-3"},
		dcLocationConfig: map[string]string{
			"pd1": "dc-1",
			"pd2": "dc-1",
			"pd3": "dc-2",
			"pd4": "dc-2",
			"pd5": "dc-3",
			"pd6": "dc-3",
		},
	}
	serverNumber := len(testCase.dcLocationConfig)
	cluster, err := tests.NewTestCluster(ctx, serverNumber, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = testCase.dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())

	cluster.WaitAllLeaders(re, testCase.dcLocationConfig)

	tsoAllocatorManager := cluster.GetServer("pd1").GetTSOAllocatorManager()
	for _, dcLocation := range testCase.dcLocations {
		suffixResp, err := etcdutil.EtcdKVGet(
			cluster.GetEtcdClient(),
			tsoAllocatorManager.GetLocalTSOSuffixPath(dcLocation))
		re.NoError(err)
		re.Len(suffixResp.Kvs, 1)
		// Test the increment of the suffix
		allSuffixResp, err := etcdutil.EtcdKVGet(
			cluster.GetEtcdClient(),
			tsoAllocatorManager.GetLocalTSOSuffixPathPrefix(),
			clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByValue, clientv3.SortAscend))
		re.NoError(err)
		re.Len(allSuffixResp.Kvs, len(testCase.dcLocations))
		var lastSuffixNum int64
		for _, kv := range allSuffixResp.Kvs {
			suffixNum, err := strconv.ParseInt(string(kv.Value), 10, 64)
			re.NoError(err)
			re.Greater(suffixNum, lastSuffixNum)
			lastSuffixNum = suffixNum
		}
	}
}

func TestNextLeaderKey(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tso.PriorityCheck = 5 * time.Second
	defer func() {
		tso.PriorityCheck = time.Minute
	}()
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-1",
	}
	serverNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(ctx, serverNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	re.NoError(err)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/injectNextLeaderKey", "return(true)"))
	re.NoError(cluster.RunInitialServers())

	cluster.WaitLeader(tests.WithWaitInterval(5*time.Second), tests.WithRetryTimes(3))
	// To speed up the test, we force to do the check
	cluster.CheckClusterDCLocation()
	originName := cluster.WaitAllocatorLeader("dc-1", tests.WithRetryTimes(5), tests.WithWaitInterval(5*time.Second))
	re.Equal("", originName)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/injectNextLeaderKey"))
	cluster.CheckClusterDCLocation()
	originName = cluster.WaitAllocatorLeader("dc-1")
	re.NotEqual("", originName)
	for name, server := range cluster.GetServers() {
		if name == originName {
			continue
		}
		err := server.GetTSOAllocatorManager().TransferAllocatorForDCLocation("dc-1", server.GetServer().GetMember().ID())
		re.NoError(err)
		testutil.Eventually(re, func() bool {
			cluster.CheckClusterDCLocation()
			currName := cluster.WaitAllocatorLeader("dc-1")
			return currName == name
		}, testutil.WithTickInterval(time.Second))
		return
	}
}
