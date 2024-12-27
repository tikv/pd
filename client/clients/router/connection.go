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

package router

import (
	"context"
	"sync"

	"golang.org/x/exp/rand"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

type connectionCtx struct {
	ctx    context.Context
	cancel context.CancelFunc
	// Current URL of the stream connection.
	streamURL string
	// Current stream to send the gRPC request.
	stream pdpb.PD_QueryRegionClient
}

type connectionManager struct {
	sync.RWMutex
	connectionCtxs map[string]*connectionCtx
}

func newConnectionManager() *connectionManager {
	return &connectionManager{
		connectionCtxs: make(map[string]*connectionCtx, 3),
	}
}

// storeIfNotExist is used to store the connection context if it does not exist before.
func (c *connectionManager) storeIfNotExist(ctx context.Context, url string, cc *grpc.ClientConn) error {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()
	_, ok := c.connectionCtxs[url]
	if ok {
		return nil
	}
	cctx, cancel := context.WithCancel(ctx)
	stream, err := pdpb.NewPDClient(cc).QueryRegion(cctx)
	if err != nil {
		cancel()
		return err
	}
	c.connectionCtxs[url] = &connectionCtx{cctx, cancel, url, stream}
	return nil
}

// deleteConnectionCtx is used to delete a connection context from the connection context map.
func (c *connectionManager) deleteConnectionCtx(url string) {
	c.Lock()
	defer c.Unlock()
	cc, ok := c.connectionCtxs[url]
	if !ok {
		return
	}
	cc.cancel()
	delete(c.connectionCtxs, url)
}

// chooseConnectionCtx is used to choose a connection context from the connection context map.
// It uses the reservoir sampling algorithm to randomly choose a connection context.
func (c *connectionManager) chooseConnectionCtx() *connectionCtx {
	c.RLock()
	defer c.RUnlock()
	idx := 0
	var connectionCtx *connectionCtx
	for _, cc := range c.connectionCtxs {
		j := rand.Intn(idx + 1)
		if j < 1 {
			connectionCtx = cc
		}
		idx++
	}
	return connectionCtx
}
