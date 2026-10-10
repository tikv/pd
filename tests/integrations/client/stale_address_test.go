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

package client_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

// staleAddressDialer simulates a stale DNS record: once redirected, dialing the
// PD address reaches another service which reused the address.
type staleAddressDialer struct {
	redirectAddr atomic.Pointer[string]

	mu    sync.Mutex
	conns []net.Conn
}

func (d *staleAddressDialer) dial(ctx context.Context, addr string) (net.Conn, error) {
	if redirectAddr := d.redirectAddr.Load(); redirectAddr != nil {
		addr = *redirectAddr
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.conns = append(d.conns, conn)
	return conn, nil
}

// redirect redirects the following dials to the given address, and breaks the
// established connections to simulate the old PD instance going away.
func (d *staleAddressDialer) redirect(addr string) {
	d.redirectAddr.Store(&addr)
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, conn := range d.conns {
		_ = conn.Close()
	}
	d.conns = nil
}

// recover makes the following dials reach the PD address again, i.e., the DNS
// record is refreshed.
func (d *staleAddressDialer) recover() {
	d.redirectAddr.Store(nil)
}

// startNonPDServer starts a gRPC server which does not provide the PD service,
// e.g., a scheduling service instance which reused the address of a PD.
func startNonPDServer(t *testing.T) string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func TestClientRecoversFromAddressReusedByOtherService(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	endpoints := runServer(re, cluster)

	dialer := &staleAddressDialer{}
	cli := setupCli(ctx, re, endpoints,
		opt.WithGRPCDialOptions(grpc.WithContextDialer(dialer.dial)))
	defer cli.Close()

	requestsSucceed := func() bool {
		if _, _, err := cli.GetTS(ctx); err != nil {
			t.Log(err)
			return false
		}
		if _, err := cli.GetAllStores(ctx); err != nil {
			t.Log(err)
			return false
		}
		return true
	}
	testutil.Eventually(re, requestsSucceed)

	// The PD address is reused by another service, and the stale DNS record
	// makes the client reconnect to it.
	dialer.redirect(startNonPDServer(t))
	testutil.Eventually(re, func() bool {
		_, err := cli.GetAllStores(ctx)
		return err != nil && strings.Contains(err.Error(), "unknown service pdpb.PD")
	})

	// After the DNS record is refreshed, the client must not keep using the
	// connection to the wrong service.
	dialer.recover()
	testutil.Eventually(re, requestsSucceed)
}
