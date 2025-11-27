// Copyright 2024 TiKV Project Authors.
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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/tests"
)

// TestShutdownDeadlock reproduces the shutdown deadlock reported in Issue 9800.
//
// The deadlock occurs when RaftCluster.Stop() holds the write lock while
// runServiceCheckJob() repeatedly tries to acquire a read lock. If shutdown
// takes longer than 30 seconds, go-deadlock detector triggers POTENTIAL DEADLOCK.
//
// This test enables two failpoints to guarantee consistent reproduction:
// 1. highFrequencyClusterJobs: Makes runServiceCheckJob tick every 1ms (vs normal 10s)
// 2. slowStopSchedulingJobs: Forces Stop() to hold the lock for 35s (exceeds 30s timeout)
//
// To run this test (from the repository root directory):
//
//	# Enable failpoint code transformation
//	failpoint-ctl enable server/cluster
//
//	# Run the test from the repository root
//	cd tests/integrations/mcs/scheduling
//	go test -tags deadlock -race -run TestShutdownDeadlock -v -timeout 3m
//
//	# Disable failpoints when done
//	cd ../../../../  # Return to repo root
//	failpoint-ctl disable server/cluster
func TestShutdownDeadlock(t *testing.T) {
	re := require.New(t)

	// Enable failpoints to reproduce the deadlock consistently
	// If these failpoints are not enabled, the test may pass intermittently
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/slowStopSchedulingJobs", `return(true)`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/slowStopSchedulingJobs"))
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy() // Deadlock detection happens here

	re.NoError(cluster.RunInitialServers())
	leaderName := cluster.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeader := cluster.GetServer(leaderName)
	re.NoError(pdLeader.BootstrapCluster())

	// Wait briefly to ensure background jobs are running
	// This allows runServiceCheckJob to start acquiring RLocks before we call Destroy()
	time.Sleep(100 * time.Millisecond)
}
