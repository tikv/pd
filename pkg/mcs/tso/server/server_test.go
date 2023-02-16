// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

func TestTSOServerStartAndStopNormally(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Expected no panic, but something bad occurred")
		}
	}()

	re := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leaderName := cluster.WaitLeader()
	leader := cluster.GetServer(leaderName)

	cfg, err := newTestDefaultConfig()
	re.NoError(err)
	cfg.BackendEndpoints = leader.GetAddr()
	cfg.ListenAddr = strings.TrimPrefix(tempurl.Alloc(), "http://")

	s, cleanup, err := newTestServer(ctx, cancel, re, cfg)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return s.IsServing()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))
	cleanup()
}
