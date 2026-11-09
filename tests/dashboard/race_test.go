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

//go:build !without_dashboard

package dashboard_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/dashboard"
	"github.com/tikv/pd/tests"
)

func TestCancelDuringStarting(t *testing.T) {
	prepareTestConfig()
	defer resetTestConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	re := require.New(t)
	starting := make(chan struct{})
	notifyStarting := sync.OnceFunc(func() { close(starting) })
	const hook = "github.com/tikv/pd/pkg/dashboard/adapter/beforeDashboardServiceStart"
	re.NoError(failpoint.EnableCall(hook, func(ctx context.Context) {
		notifyStarting()
		<-ctx.Done()
	}))
	t.Cleanup(func() { re.NoError(failpoint.Disable(hook)) })
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer func() {
		cancel()
		cluster.Destroy()
	}()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())

	select {
	case <-starting:
	case <-time.After(5 * time.Second):
		t.Fatal("dashboard did not reach service startup")
	}
	cancel()
}

func prepareTestConfig() {
	dashboard.SetCheckInterval(50 * time.Millisecond)
	tests.WaitLeaderReturnDelay = 0
	tests.WaitLeaderCheckInterval = 20 * time.Millisecond
}

func resetTestConfig() {
	dashboard.SetCheckInterval(time.Second)
	tests.WaitLeaderReturnDelay = 20 * time.Millisecond
	tests.WaitLeaderCheckInterval = 500 * time.Millisecond
}
