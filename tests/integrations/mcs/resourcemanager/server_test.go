// Copyright 2023 TiKV Project Authors.
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

package resourcemanager_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	meteringconfig "github.com/pingcap/metering_sdk/config"
	meteringreader "github.com/pingcap/metering_sdk/reader/metering"
	meteringstorage "github.com/pingcap/metering_sdk/storage"

	bs "github.com/tikv/pd/pkg/basicserver"
	rmserver "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/metering"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestResourceManagerServer(t *testing.T) {
	re := require.New(t)

	ctx := t.Context()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leaderName := cluster.WaitLeader()
	re.NotEmpty(leaderName)
	leader := cluster.GetServer(leaderName)

	s, cleanup := tests.StartSingleResourceManagerTestServer(ctx, re, leader.GetAddr(), tempurl.Alloc())
	addr := s.GetAddr()
	defer cleanup()
	tests.WaitForPrimaryServing(re, map[string]bs.Server{addr: s})

	// Test registered GRPC Service
	cc, err := grpcutil.GetClientConn(ctx, addr, nil)
	re.NoError(err)
	defer cc.Close()

	c := rmpb.NewResourceManagerClient(cc)
	_, err = c.GetResourceGroup(context.Background(), &rmpb.GetResourceGroupRequest{
		ResourceGroupName: "pingcap",
	})
	re.ErrorContains(err, "resource group does not exist")

	// Test registered REST HTTP Handler
	url := addr + "/resource-manager/api/v1/config"
	{
		resp, err := tests.TestDialClient.Get(url + "/groups")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respString, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.JSONEq(`[{"name":"default","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":2147483647,"burst_limit":-1},"state":{"initialized":false}}},"priority":8}]`, string(respString))
	}
	{
		group := &rmpb.ResourceGroup{
			Name: "pingcap",
			Mode: 1,
		}
		createJSON, err := json.Marshal(group)
		re.NoError(err)
		resp, err := tests.TestDialClient.Post(url+"/group", "application/json", strings.NewReader(string(createJSON)))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	}
	{
		resp, err := tests.TestDialClient.Get(url + "/group/pingcap")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respString, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.JSONEq("{\"name\":\"pingcap\",\"mode\":1,\"r_u_settings\":{\"r_u\":{\"state\":{\"initialized\":false}}},\"priority\":0}", string(respString))
	}

	// Test metrics handler
	{
		resp, err := tests.TestDialClient.Get(addr + "/metrics")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respBytes, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.Contains(string(respBytes), "pd_server_info")
	}

	// Test status handler
	{
		resp, err := tests.TestDialClient.Get(addr + "/status")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respBytes, err := io.ReadAll(resp.Body)
		re.NoError(err)
		var s versioninfo.Status
		re.NoError(json.Unmarshal(respBytes, &s))
		re.Equal(versioninfo.PDBuildTS, s.BuildTS)
		re.Equal(versioninfo.PDGitHash, s.GitHash)
		re.Equal(versioninfo.PDReleaseVersion, s.Version)
	}

	// Test health handler
	{
		resp, err := tests.TestDialClient.Get(addr + "/health")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	}
}

type shutdownMeteringProbe struct {
	once       sync.Once
	aggregated chan struct{}
}

func (*shutdownMeteringProbe) Category() string { return "shutdown-probe" }
func (*shutdownMeteringProbe) Collect(any)      {}
func (p *shutdownMeteringProbe) Aggregate() []map[string]any {
	p.once.Do(func() { close(p.aggregated) })
	return nil
}

func TestShutdownFlushesAcceptedConsumption(t *testing.T) {
	for _, standalone := range []bool{false, true} {
		name := "embedded"
		if standalone {
			name = "standalone"
		}
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			meteringCfg := meteringconfig.NewMeteringConfig().WithLocalFS(t.TempDir())
			cluster, err := tests.NewTestCluster(ctx, 1, func(cfg *config.Config, _ string) {
				if !standalone {
					cfg.Metering = *meteringCfg
				}
			})
			re.NoError(err)
			defer cluster.Destroy()
			re.NoError(cluster.RunInitialServers())
			leader := cluster.GetServer(cluster.WaitLeader())
			var srv bs.Server = leader.GetServer()
			writer := leader.GetServer().GetMeteringWriter()
			if standalone {
				cfg := rmserver.NewConfig()
				cfg.BackendEndpoints = leader.GetAddr()
				cfg.ListenAddr = tempurl.Alloc()
				cfg.Name = cfg.ListenAddr
				cfg, err = rmserver.GenerateConfig(cfg)
				re.NoError(err)
				cfg.Metering = *meteringCfg
				rm, cleanup, err := rmserver.NewTestServer(ctx, re, cfg)
				re.NoError(err)
				defer cleanup()
				tests.WaitForPrimaryServing(re, map[string]bs.Server{rm.GetAddr(): rm})
				srv = rm
				writer = rm.GetMeteringWriter()
			}
			re.NotNil(writer)
			cc, err := grpcutil.GetClientConn(context.Background(), srv.GetAddr(), nil)
			re.NoError(err)
			defer cc.Close()
			client := rmpb.NewResourceManagerClient(cc)
			processing := make(chan struct{})
			release := make(chan struct{})
			var processingOnce, releaseOnce sync.Once
			const blocker = "github.com/tikv/pd/pkg/mcs/resourcemanager/server/beforeProcessConsumption"
			re.NoError(failpoint.EnableCall(blocker, func(recordMetrics bool) {
				if recordMetrics {
					processingOnce.Do(func() { close(processing) })
					<-release
				}
			}))
			defer func() {
				releaseOnce.Do(func() { close(release) })
				re.NoError(failpoint.Disable(blocker))
			}()
			stream, err := client.AcquireTokenBuckets(context.Background())
			re.NoError(err)
			re.NoError(stream.Send(&rmpb.TokenBucketsRequest{
				Requests:              []*rmpb.TokenBucketRequest{{ResourceGroupName: "default", ConsumptionSinceLastRequest: &rmpb.Consumption{RRU: 2}}},
				TargetRequestPeriodMs: 1000, ClientUniqueId: 1,
			}))
			_, err = stream.Recv()
			re.NoError(err)
			re.NoError(stream.CloseSend())
			select {
			case <-processing:
			case <-time.After(5 * time.Second):
				t.Fatal("accepted consumption did not reach the worker")
			}
			probe := &shutdownMeteringProbe{aggregated: make(chan struct{})}
			writer.RegisterCollector(probe)
			// Both command entry points cancel this parent before calling Close.
			cancel()
			closed := make(chan struct{})
			go func() {
				srv.Close()
				close(closed)
			}()
			// Expose an early final aggregate while accepted work is still blocked.
			// A periodic aggregate is also allowed; Close must export the later work.
			select {
			case <-probe.aggregated:
			case <-closed:
				t.Error("server closed before accepted consumption finished")
			case <-time.After(time.Second):
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case <-closed:
			case <-time.After(15 * time.Second):
				t.Fatal("server did not finish draining accepted consumption")
			}
			provider, err := meteringstorage.NewObjectStorageProvider(meteringCfg.ToProviderConfig())
			re.NoError(err)
			reader := meteringreader.NewMeteringReader(provider, meteringconfig.DefaultConfig())
			defer reader.Close()
			files, err := reader.List(context.Background(), "")
			re.NoError(err)
			var totalRU float64
			for _, file := range files {
				data, err := reader.ReadFile(context.Background(), file)
				re.NoError(err)
				if data.Category != rmserver.ResourceManagerCategory {
					continue
				}
				for _, record := range data.Data {
					re.IsType("", record[metering.DataClusterIDField])
					re.Empty(record[metering.DataClusterIDField])
					ru := record["oltp_ru"].(map[string]any)
					re.Equal(metering.UnitRU, ru["unit"])
					totalRU += ru["value"].(float64)
				}
			}
			re.Equal(2.0, totalRU, "all accepted consumption must reach the final metering export")
		})
	}
}
