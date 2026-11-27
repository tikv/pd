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

package pd

import (
	"context"
	"maps"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/tikv/pd/client/clients/tso"
	"github.com/tikv/pd/client/metrics"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/circuitbreaker"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
	resourcegroupmetrics "github.com/tikv/pd/client/resource_group/controller/metrics"
	sd "github.com/tikv/pd/client/servicediscovery"
)

type delayedDialServiceDiscovery struct {
	sd.ServiceDiscovery
	dialStarted  chan struct{}
	resumeDial   chan struct{}
	dialFinished chan struct{}
	dialOnce     sync.Once
	dialErr      error
	clientConns  sync.Map
}

func (*delayedDialServiceDiscovery) ExecAndAddLeaderSwitchedCallback(callback sd.LeaderSwitchedCallbackFunc) {
	_ = callback("http://127.0.0.1:1")
}

func (s *delayedDialServiceDiscovery) GetOrCreateGRPCConn(url string) (*grpc.ClientConn, error) {
	s.dialOnce.Do(func() {
		close(s.dialStarted)
		<-s.resumeDial
		_, s.dialErr = grpcutil.GetOrCreateGRPCConn(context.Background(), &s.clientConns, url, nil)
		close(s.dialFinished)
	})
	if s.dialErr != nil {
		return nil, s.dialErr
	}
	// The update was canceled while dialing, so no TSO stream is needed.
	return nil, context.Canceled
}

func (s *delayedDialServiceDiscovery) Close() {
	s.clientConns.Range(func(_, value any) bool {
		_ = value.(*grpc.ClientConn).Close()
		return true
	})
}

func TestCloseWaitsForTSOConnectionsBeforeClosingDiscovery(t *testing.T) {
	re := require.New(t)
	discovery := &delayedDialServiceDiscovery{
		ServiceDiscovery: sd.NewMockServiceDiscovery(nil, nil),
		dialStarted:      make(chan struct{}),
		resumeDial:       make(chan struct{}),
		dialFinished:     make(chan struct{}),
	}
	resumeDial := sync.OnceFunc(func() { close(discovery.resumeDial) })
	t.Cleanup(discovery.Close)
	t.Cleanup(resumeDial)
	keeper := &serviceModeKeeper{
		tsoSvcDiscovery: discovery,
		tsoClient: tso.NewClient(context.Background(), opt.NewOption(),
			discovery, &tso.MSStreamBuilderFactory{}),
	}
	t.Cleanup(func() {
		resumeDial()
		keeper.close()
	})
	keeper.tsoClient.Setup()
	select {
	case <-discovery.dialStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("TSO connection update did not start")
	}

	closed := make(chan struct{})
	go func() {
		keeper.close()
		close(closed)
	}()
	// Keep the dial in flight until shutdown either completes or waits for it.
	select {
	case <-closed:
	case <-time.After(100 * time.Millisecond):
	}
	resumeDial()
	select {
	case <-discovery.dialFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("TSO connection update did not finish")
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("TSO client did not close")
	}

	re.NoError(discovery.dialErr)
	conn, ok := discovery.clientConns.Load("http://127.0.0.1:1")
	re.True(ok)
	re.Equal(connectivity.Shutdown, conn.(*grpc.ClientConn).GetState(),
		"a connection created during shutdown must be closed")
}

func TestClientMetricsRegistration(t *testing.T) {
	const env = "PD_TEST_CLIENT_METRICS_REGISTRATION"
	mode := os.Getenv(env)
	if mode == "" {
		// Each case needs a fresh default registry and a fresh sync.Once,
		// regardless of which other client tests have already run.
		executable, err := os.Executable()
		require.NoError(t, err)
		for _, mode := range []string{"default", "labeled"} {
			t.Run(mode, func(t *testing.T) {
				t.Setenv(env, mode)
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, executable, "-test.run=^TestClientMetricsRegistration$")
				output, err := cmd.CombinedOutput()
				require.NoError(t, err, "%s", output)
			})
		}
		return
	}

	addr, _, cleanup := startTestRMServer(t, "pd")
	t.Cleanup(cleanup)
	newClient := func() *innerClient {
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		c := newInnerClientForRMRouteTest(t, ctx, addr)
		c.cancel = cancel
		t.Cleanup(c.close)
		c.option = opt.NewOption()
		return c
	}
	c := newClient()
	opt.WithInitMetricsOption(false)(c.option) //nolint:staticcheck // Exercise the legacy opt-out.
	require.NoError(t, c.setup())

	breaker := circuitbreaker.NewCircuitBreaker("registration_test", circuitbreaker.AlwaysClosedSettings)
	record := func() {
		metrics.TSOBatchSize.Observe(1)
		metrics.CmdDurationTSO.Observe(1)
		metrics.OngoingRequestCountGauge.WithLabelValues("s1").Inc()
		resourcegroupmetrics.FailedRequestCounter.WithLabelValues("rg", "rg", "err").Inc()
		resourcegroupmetrics.FailedTokenRequestDuration.Observe(1)
		if err := breaker.Execute(func() (circuitbreaker.Overloading, error) {
			return circuitbreaker.No, nil
		}); err != nil {
			t.Error(err)
		}
	}
	record()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		require.False(t, strings.HasPrefix(family.GetName(), "pd_client_"), family.GetName())
		require.False(t, strings.HasPrefix(family.GetName(), "resource_manager_client_"), family.GetName())
	}

	var wantLabels prometheus.Labels
	if mode == "labeled" {
		wantLabels = prometheus.Labels{"keyspace_name": "ks1"}
	}
	labels := maps.Clone(wantLabels)
	const workers, observations = 4, 100
	var wg sync.WaitGroup
	start := make(chan struct{})
	var setupErr error
	if mode == "labeled" {
		labeledClient := newClient()
		opt.WithMetricsLabels(labels)(labeledClient.option) //nolint:staticcheck // Exercise legacy client registration.
		wg.Go(func() {
			<-start
			setupErr = labeledClient.setup()
		})
	}
	for range workers {
		wg.Go(func() {
			<-start
			for range observations {
				record()
			}
		})
		if mode == "default" {
			wg.Go(func() {
				<-start
				metrics.InitAndRegisterMetrics(labels)
			})
		}
	}
	close(start)
	wg.Wait()
	require.NoError(t, setupErr)
	if mode == "labeled" {
		labels["keyspace_name"] = "mutated"
	}
	metrics.InitAndRegisterMetrics(prometheus.Labels{"keyspace_name": "ignored"})
	record()

	families, err = prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	byName := make(map[string][]*dto.Metric, len(families))
	for _, family := range families {
		byName[family.GetName()] = family.GetMetric()
	}
	findMetric := func(name string, matchName, matchValue string) *dto.Metric {
		t.Helper()
		for _, sample := range byName[name] {
			matches := matchName == ""
			var gotLabels prometheus.Labels
			for _, label := range sample.GetLabel() {
				if label.GetName() == matchName && label.GetValue() == matchValue {
					matches = true
				}
				if label.GetName() == "keyspace_name" {
					gotLabels = prometheus.Labels{"keyspace_name": label.GetValue()}
				}
			}
			if matches {
				require.Equal(t, wantLabels, gotLabels, name)
				return sample
			}
		}
		t.Fatalf("sample not found for %s (%s=%q)", name, matchName, matchValue)
		return nil
	}
	const want = workers*observations + 2
	batch := findMetric("pd_client_request_handle_tso_batch_size", "", "")
	require.EqualValues(t, want, batch.GetHistogram().GetSampleCount())
	command := findMetric("pd_client_cmd_handle_cmds_duration_seconds", "type", "tso")
	require.EqualValues(t, want, command.GetHistogram().GetSampleCount())
	ongoing := findMetric("pd_client_request_ongoing_requests_count", "stream", "s1")
	require.InDelta(t, want, ongoing.GetGauge().GetValue(), 0)
	circuit := findMetric("pd_client_request_circuit_breaker_count", "event", "success")
	require.InDelta(t, want, circuit.GetCounter().GetValue(), 0)
	failed := findMetric("resource_manager_client_request_fail", "resource_group", "rg")
	require.InDelta(t, want, failed.GetCounter().GetValue(), 0)
	token := findMetric("resource_manager_client_token_request_duration", "type", "fail")
	require.EqualValues(t, want, token.GetHistogram().GetSampleCount())
}
