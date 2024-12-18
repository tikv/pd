// Copyright 2016 TiKV Project Authors.
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

package id_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

const allocStep = uint64(1000)

func TestID(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())

	leaderServer := cluster.GetLeaderServer()
	var last uint64
	for range allocStep {
		id, err := leaderServer.GetAllocator().Alloc()
		re.NoError(err)
		re.Greater(id, last)
		last = id
	}

	var wg sync.WaitGroup

	var m syncutil.Mutex
	ids := make(map[uint64]struct{})

	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for range 200 {
				id, err := leaderServer.GetAllocator().Alloc()
				re.NoError(err)
				m.Lock()
				_, ok := ids[id]
				ids[id] = struct{}{}
				m.Unlock()
				re.False(ok)
			}
		}()
	}

	wg.Wait()
}

func TestCommand(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())

	leaderServer := cluster.GetLeaderServer()
	req := &pdpb.AllocIDRequest{Header: testutil.NewRequestHeader(leaderServer.GetClusterID())}

	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	var last uint64
	for range 2 * allocStep {
		resp, err := grpcPDClient.AllocID(context.Background(), req)
		re.NoError(err)
		re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
		re.Greater(resp.GetId(), last)
		last = resp.GetId()
	}
}

func TestMonotonicID(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())

	leaderServer := cluster.GetLeaderServer()
	var last1 uint64
	for range 10 {
		id, err := leaderServer.GetAllocator().Alloc()
		re.NoError(err)
		re.Greater(id, last1)
		last1 = id
	}
	err = cluster.ResignLeader()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	leaderServer = cluster.GetLeaderServer()
	var last2 uint64
	for range 10 {
		id, err := leaderServer.GetAllocator().Alloc()
		re.NoError(err)
		re.Greater(id, last2)
		last2 = id
	}
	err = cluster.ResignLeader()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	leaderServer = cluster.GetLeaderServer()
	id, err := leaderServer.GetAllocator().Alloc()
	re.NoError(err)
	re.Greater(id, last2)
	var last3 uint64
	for range 1000 {
		id, err := leaderServer.GetAllocator().Alloc()
		re.NoError(err)
		re.Greater(id, last3)
		last3 = id
	}
}

func TestPDRestart(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	leaderServer := cluster.GetLeaderServer()

	var last uint64
	for range 10 {
		id, err := leaderServer.GetAllocator().Alloc()
		re.NoError(err)
		re.Greater(id, last)
		last = id
	}

	re.NoError(leaderServer.Stop())
	re.NoError(leaderServer.Run())
	re.NotEmpty(cluster.WaitLeader())

	for range 10 {
		id, err := leaderServer.GetAllocator().Alloc()
		re.NoError(err)
		re.Greater(id, last)
		last = id
	}
}
