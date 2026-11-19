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

package tso

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// TestAllocatorManagerClosesConnectionsAddedDuringShutdown checks connection cleanup races.
func TestAllocatorManagerClosesConnectionsAddedDuringShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	am := &AllocatorManager{
		ctx:    ctx,
		cancel: cancel,
	}
	am.localAllocatorConn.clientConns = make(map[string]*grpc.ClientConn)

	const connCount = 8
	conns := make([]*grpc.ClientConn, connCount)
	for i := range conns {
		conn, err := grpc.NewClient(
			fmt.Sprintf("passthrough:///allocator-%d", i),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		conns[i] = conn
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(connCount + 1)
	for i, conn := range conns {
		go func(i int, conn *grpc.ClientConn) {
			defer wg.Done()
			<-start
			am.setGRPCConn(conn, fmt.Sprintf("allocator-%d", i))
		}(i, conn)
	}
	go func() {
		defer wg.Done()
		<-start
		am.Close()
	}()
	close(start)
	wg.Wait()

	for _, conn := range conns {
		require.Equal(t, connectivity.Shutdown, conn.GetState())
	}
}
