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
	"errors"
	"flag"
	"net"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	clienterrs "github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
)

type testKeyspaceServer struct {
	keyspacepb.UnimplementedKeyspaceServer

	meta     *keyspacepb.KeyspaceMeta
	respType pdpb.ErrorType
	respMsg  string
	rpcErr   error
	lastID   atomic.Uint32
}

func (s *testKeyspaceServer) LoadKeyspaceByID(
	_ context.Context,
	req *keyspacepb.LoadKeyspaceByIDRequest,
) (*keyspacepb.LoadKeyspaceResponse, error) {
	s.lastID.Store(req.GetId())
	if s.rpcErr != nil {
		return nil, s.rpcErr
	}
	resp := &keyspacepb.LoadKeyspaceResponse{
		Header: &pdpb.ResponseHeader{},
	}
	if s.respMsg != "" {
		resp.Header.Error = &pdpb.Error{
			Type:    s.respType,
			Message: s.respMsg,
		}
		return resp, nil
	}
	resp.Keyspace = s.meta
	return resp, nil
}

func startTestKeyspaceServer(t *testing.T, keyspaceServer *testKeyspaceServer) (string, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	keyspacepb.RegisterKeyspaceServer(server, keyspaceServer)

	serveErr := make(chan error, 1)
	go func() {
		defer close(serveErr)
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serveErr <- err
		}
	}()

	addr := "http://" + listener.Addr().String()
	cleanup := func() {
		server.Stop()
		require.NoError(t, <-serveErr)
	}
	return addr, cleanup
}

func newTestKeyspaceClient(t *testing.T, ctx context.Context, addr string) *client {
	t.Helper()

	conn, err := grpcutil.GetClientConn(ctx, addr, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	option := opt.NewOption()
	option.InitMetrics = false
	return &client{
		inner: &innerClient{
			serviceDiscovery: newTestServiceDiscovery(addr, conn),
			ctx:              ctx,
			option:           option,
		},
	}
}

func TestLoadKeyspaceByID(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	expected := &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42},
		Name:     "test-keyspace",
		State:    keyspacepb.KeyspaceState_ENABLED,
	}
	keyspaceServer := &testKeyspaceServer{meta: expected}
	addr, cleanup := startTestKeyspaceServer(t, keyspaceServer)
	t.Cleanup(cleanup)

	client := newTestKeyspaceClient(t, ctx, addr)
	span, tracedCtx := opentracing.StartSpanFromContext(ctx, "test-load-keyspace-by-id")
	defer span.Finish()
	loaded, err := client.LoadKeyspaceByID(tracedCtx, expected.GetId())
	re.NoError(err)
	re.Equal(expected, loaded)
	re.Equal(expected.GetId(), keyspaceServer.lastID.Load())
}

func TestLoadKeyspaceByIDReturnsHeaderError(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	keyspaceServer := &testKeyspaceServer{
		respType: pdpb.ErrorType_ENTRY_NOT_FOUND,
		respMsg:  "keyspace does not exist",
	}
	addr, cleanup := startTestKeyspaceServer(t, keyspaceServer)
	t.Cleanup(cleanup)

	client := newTestKeyspaceClient(t, ctx, addr)
	loaded, err := client.LoadKeyspaceByID(ctx, 42)
	re.Nil(loaded)
	re.ErrorContains(err, "Load keyspace id 42 failed")
	re.ErrorContains(err, "keyspace does not exist")
	re.Equal(uint32(42), keyspaceServer.lastID.Load())
}

func TestLoadKeyspaceByIDReturnsProtoClientError(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	option := opt.NewOption()
	client := &client{
		inner: &innerClient{
			serviceDiscovery: &testServiceDiscovery{},
			ctx:              ctx,
			option:           option,
		},
	}

	loaded, err := client.LoadKeyspaceByID(ctx, 42)
	re.Nil(loaded)
	re.True(clienterrs.ErrClientGetProtoClient.Equal(err))
}

func TestLoadKeyspaceByIDReturnsRPCError(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	keyspaceServer := &testKeyspaceServer{
		rpcErr: errors.New("load keyspace by id failed"),
	}
	addr, cleanup := startTestKeyspaceServer(t, keyspaceServer)
	t.Cleanup(cleanup)

	client := newTestKeyspaceClient(t, ctx, addr)
	loaded, err := client.LoadKeyspaceByID(ctx, 42)
	re.Nil(loaded)
	re.ErrorContains(err, "load keyspace by id failed")
	re.Equal(uint32(42), keyspaceServer.lastID.Load())
}

func TestClientMetricsInitializationIsConcurrentSafe(t *testing.T) {
	// Each invocation must exercise the first explicit metrics initialization.
	if os.Getenv("PD_CLIENT_METRICS_TEST") != t.Name() {
		testBinary, err := os.Executable()
		require.NoError(t, err)
		// #nosec G204 -- Re-execute the current test binary without a shell.
		cmd := exec.CommandContext(t.Context(), testBinary, "-test.run=^"+t.Name()+"$", "-test.count=1")
		cmd.Env = append(os.Environ(), "PD_CLIENT_METRICS_TEST="+t.Name())
		if coverDir := flag.Lookup("test.gocoverdir").Value.String(); coverDir != "" {
			cmd.Args = append(cmd.Args, "-test.gocoverdir="+coverDir)
		}
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}

	expected := &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42},
		Name:     "test-keyspace",
	}
	addr, cleanup := startTestKeyspaceServer(t, &testKeyspaceServer{meta: expected})
	t.Cleanup(cleanup)
	client := newTestKeyspaceClient(t, t.Context(), addr)
	load := func() error {
		loaded, err := client.LoadKeyspaceByID(t.Context(), expected.GetId())
		if err != nil {
			return err
		}
		if loaded.GetId() != expected.GetId() {
			return errors.New("unexpected keyspace ID")
		}
		return nil
	}

	const producerCount = 32
	before := metrics.CmdDurationLoadKeyspaceByID
	var (
		ready sync.WaitGroup
		wg    sync.WaitGroup
		stop  atomic.Bool
	)
	errorsCh := make(chan error, producerCount)
	t.Cleanup(func() {
		stop.Store(true)
		wg.Wait()
	})
	ready.Add(producerCount)
	wg.Add(producerCount)
	for range producerCount {
		go func() {
			defer wg.Done()
			err := load()
			ready.Done()
			for err == nil && !stop.Load() {
				err = load()
			}
			if err != nil {
				errorsCh <- err
			}
		}()
	}
	ready.Wait()
	metrics.InitAndRegisterMetrics(prometheus.Labels{"instance": "test"})
	stop.Store(true)
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}
	require.NotSame(t, before, metrics.CmdDurationLoadKeyspaceByID)
	require.NoError(t, load())

	// Public collectors must retain their labels and support independent registries.
	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.TSORetryCount)
	metrics.TSORetryCount.Observe(1)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Len(t, families[0].GetMetric(), 1)
	labels := families[0].GetMetric()[0].GetLabel()
	require.Len(t, labels, 1)
	require.Equal(t, "instance", labels[0].GetName())
	require.Equal(t, "test", labels[0].GetValue())
	require.True(t, registry.Unregister(metrics.TSORetryCount))
}
