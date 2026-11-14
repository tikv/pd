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

package tso

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/tests"
)

func TestPDClientUsesTSODuringPDInitialization(t *testing.T) {
	re := require.New(t)
	cluster, err := tests.NewTestCluster(t.Context(), 3)
	re.NoError(err)
	t.Cleanup(cluster.Destroy)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	re.NoError(leader.BootstrapCluster())
	target := cluster.GetServer(cluster.GetFollower())
	re.NotNil(target)

	// Keep the default client alive across the handoff, as existing TiDB and
	// TiKV clients do during a PD upgrade. Constructing a new client also
	// initializes router metadata, which still requires full PD readiness.
	clientCtx, clientCancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(clientCancel)
	client, err := pd.NewClientWithContext(clientCtx, caller.TestComponent,
		[]string{target.GetAddr()}, pd.SecurityOption{},
		opt.WithCustomTimeoutOption(time.Second), opt.WithMaxErrorRetry(1))
	re.NoError(err)
	t.Cleanup(client.Close)
	re.Equal(leader.GetAddr(), client.GetLeaderURL())
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	physical, logical, err := client.GetTS(ctx)
	cancel()
	re.NoError(err)
	previous := &pdpb.Timestamp{Physical: physical, Logical: logical}

	const hook = "github.com/tikv/pd/server/cluster/afterTSOInitialized"
	reached := make(chan struct{})
	release := make(chan struct{})
	var reachedOnce sync.Once
	resume := sync.OnceFunc(func() { close(release) })
	memberID := target.GetServerID()
	re.NoError(failpoint.EnableCall(hook, func(id uint64) {
		if id == memberID {
			reachedOnce.Do(func() { close(reached) })
			<-release
		}
	}))
	t.Cleanup(func() {
		resume()
		re.NoError(failpoint.Disable(hook))
	})
	re.NoError(leader.MoveEtcdLeader(leader.GetServerID(), target.GetServerID()))
	select {
	case <-reached:
	case <-time.After(30 * time.Second):
		t.Fatal("leader initialization did not reach the TSO checkpoint")
	}
	re.False(target.GetServer().IsServing())

	// Recovery includes discovering the new owner and reconnecting its TSO
	// stream while that owner's metadata initialization remains blocked.
	testutil.Eventually(re, func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		physical, logical, err = client.GetTS(ctx)
		return err == nil
	}, testutil.WithWaitFor(5*time.Second))
	re.Equal(target.GetAddr(), client.GetLeaderURL())
	re.Positive(physical)
	re.Positive(logical)
	next := &pdpb.Timestamp{Physical: physical, Logical: logical}
	re.Negative(tsoutil.CompareTimestamp(previous, next))
	re.False(target.GetServer().IsServing())

	resume()
	testutil.Eventually(re, target.GetServer().IsServing)
}
