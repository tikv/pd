// Copyright 2025 TiKV Project Authors.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/tests"
)

// Ensure WaitLoad failure won't leave background goroutines running.
func TestNewWatcherWaitLoadFailed(t *testing.T) {
	re := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestClusterWithKeyspaceGroup(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	leaderName := cluster.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeader := cluster.GetServer(leaderName)
	re.NoError(pdLeader.BootstrapCluster())

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/loadTemporaryFail", "return(3)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/loadTemporaryFail"))
	}()
	tc, err := tests.NewTestSchedulingCluster(ctx, 1, cluster)
	if err == nil && tc != nil {
		defer tc.Destroy()
	}
}
