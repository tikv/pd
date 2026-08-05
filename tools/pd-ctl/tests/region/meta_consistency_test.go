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

package region_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/server/config"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/pdctl/command"
)

func TestRegionMetaConsistencyUsesFollowerLocalCache(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := pdTests.NewTestCluster(ctx, 3, func(conf *config.Config, _ string) {
		conf.PDServerCfg.UseRegionStorage = true
	})
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	re.NoError(leader.BootstrapCluster())
	re.True(cluster.WaitRegionSyncerClientsReady(2))

	regions := []*core.RegionInfo{
		pdTests.MustPutRegion(re, cluster, 1, 1, []byte{}, []byte("a"), core.SetRegionVersion(1)),
		pdTests.MustPutRegion(re, cluster, 2, 1, []byte("a"), []byte("b"), core.SetRegionVersion(1)),
		pdTests.MustPutRegion(re, cluster, 3, 1, []byte("b"), []byte{}, core.SetRegionVersion(1)),
	}
	for _, server := range cluster.GetServers() {
		re.Eventually(func() bool {
			return len(server.GetServer().GetBasicCluster().GetRegions()) == len(regions)
		}, 5*time.Second, 20*time.Millisecond)
	}

	seed := cluster.GetConfig().GetClientURL()
	status, report, stderr, err := executeMetaConsistency(seed, t.TempDir())
	re.NoError(err, stderr)
	re.Equal(0, status)
	re.Equal("consistent", report["status"])

	followerName := cluster.GetFollower()
	re.NotEmpty(followerName)
	follower := cluster.GetServer(followerName)
	stale := regions[1].Clone(core.WithIncVersion())
	follower.GetServer().GetBasicCluster().PutRegion(stale)
	re.Equal(stale.GetRegionEpoch().GetVersion(),
		follower.GetServer().GetBasicCluster().GetRegion(stale.GetID()).GetRegionEpoch().GetVersion())

	status, report, stderr, err = executeMetaConsistency(seed, t.TempDir())
	re.Error(err, stderr)
	re.Equal(1, status)
	if report == nil {
		t.Fatalf("missing report: err=%v stderr=%q", err, stderr)
	}
	re.Equal("inconsistent", report["status"])
	re.Equal(float64(1), report["summary"].(map[string]any)["different_regions"])
	difference := report["differences"].([]any)[0].(map[string]any)
	re.Equal(float64(stale.GetID()), difference["region_id"])
	re.Contains(difference, "epoch")

	follower.GetServer().GetBasicCluster().PutRegion(regions[1])
	status, report, stderr, err = executeMetaConsistency(seed, t.TempDir())
	re.NoError(err, stderr)
	re.Equal(0, status)
	re.Equal("consistent", report["status"])

	follower.GetServer().GetBasicCluster().RemoveRegionIfExist(regions[1].GetID())
	status, report, stderr, err = executeMetaConsistency(seed, t.TempDir())
	re.Error(err, stderr)
	re.Equal(1, status)
	re.Equal("inconsistent", report["status"])
	difference = report["differences"].([]any)[0].(map[string]any)
	re.Equal(float64(regions[1].GetID()), difference["region_id"])
	missingOn := difference["missing_on"].([]any)
	re.Len(missingOn, 1)
	re.Contains(missingOn[0], followerName+"@")

	follower.GetServer().GetBasicCluster().PutRegion(regions[1])
}

func executeMetaConsistency(seed, workDir string) (int, map[string]any, string, error) {
	root := ctl.GetRootCmd()
	var stdout, stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetArgs([]string{
		"-u", seed, "region", "meta-consistency", "--batch-size", "2", "--interval", "0s",
		"--timeout", "2s", "--max-runtime", "30s", "--confirm-limit", "8", "--work-dir", workDir,
	})
	err := root.Execute()
	status := 0
	if err != nil {
		status, _ = command.ExitCode(err)
	}
	var report map[string]any
	if unmarshalErr := json.Unmarshal(stdout.Bytes(), &report); unmarshalErr != nil {
		err = fmt.Errorf("command error: %v; decode report: %w; stdout: %q", err, unmarshalErr, stdout.String())
	}
	return status, report, stderr.String(), err
}
