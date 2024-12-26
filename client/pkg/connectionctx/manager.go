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

package connectionctx

import (
	"context"
	"sync"

	"golang.org/x/exp/rand"
)

type connectionCtx[T any] struct {
	Ctx    context.Context
	Cancel context.CancelFunc
	// Current URL of the stream connection.
	StreamURL string
	// Current stream to send the gRPC requests.
	Stream T
}

// Manager is used to manage the connection contexts.
type Manager[T any] struct {
	sync.RWMutex
	connectionCtxs map[string]*connectionCtx[T]
}

// NewManager is used to create a new connection context manager.
func NewManager[T any]() *Manager[T] {
	return &Manager[T]{
		connectionCtxs: make(map[string]*connectionCtx[T], 3),
	}
}

// Exist is used to check if the connection context exists by the given URL.
func (c *Manager[T]) Exist(url string) bool {
	c.RLock()
	defer c.RUnlock()
	_, ok := c.connectionCtxs[url]
	return ok
}

// Store is used to store the connection context, `overwrite` is used to force the store operation
// no matter whether the connection context exists before, which is false by default.
func (c *Manager[T]) Store(ctx context.Context, url string, stream T, overwrite ...bool) {
	c.Lock()
	defer c.Unlock()
	overwriteFlag := false
	if len(overwrite) > 0 {
		overwriteFlag = overwrite[0]
	}
	_, ok := c.connectionCtxs[url]
	if !overwriteFlag && ok {
		return
	}
	c.storeLocked(ctx, url, stream)
}

func (c *Manager[T]) storeLocked(ctx context.Context, url string, stream T) {
	c.releaseLocked(url)
	cctx, cancel := context.WithCancel(ctx)
	c.connectionCtxs[url] = &connectionCtx[T]{cctx, cancel, url, stream}
}

// CleanAllAndStore is used to store the connection context exclusively. It will release
// all other connection contexts. `stream` is optional, if it is not provided, all
// connection contexts other than the given `url` will be released.
func (c *Manager[T]) CleanAllAndStore(ctx context.Context, url string, stream ...T) {
	c.Lock()
	defer c.Unlock()
	// Remove all other `connectionCtx`s.
	c.gcLocked(func(curURL string) bool {
		return curURL != url
	})
	if len(stream) == 0 {
		return
	}
	c.storeLocked(ctx, url, stream[0])
}

// GC is used to release all connection contexts that match the given condition.
func (c *Manager[T]) GC(condition func(url string) bool) {
	c.Lock()
	defer c.Unlock()
	c.gcLocked(condition)
}

func (c *Manager[T]) gcLocked(condition func(url string) bool) {
	for url := range c.connectionCtxs {
		if condition(url) {
			c.releaseLocked(url)
		}
	}
}

// ReleaseAll is used to release all connection contexts.
func (c *Manager[T]) ReleaseAll() {
	c.GC(func(string) bool { return true })
}

// Release is used to delete a connection context from the connection context map and release the resources.
func (c *Manager[T]) Release(url string) {
	c.Lock()
	defer c.Unlock()
	c.releaseLocked(url)
}

func (c *Manager[T]) releaseLocked(url string) {
	cc, ok := c.connectionCtxs[url]
	if !ok {
		return
	}
	cc.Cancel()
	delete(c.connectionCtxs, url)
}

// GetConnectionCtx is used to get a connection context from the connection context map.
// It uses the reservoir sampling algorithm to randomly pick one connection context.
func (c *Manager[T]) GetConnectionCtx() *connectionCtx[T] {
	c.RLock()
	defer c.RUnlock()
	idx := 0
	var connectionCtx *connectionCtx[T]
	for _, cc := range c.connectionCtxs {
		j := rand.Intn(idx + 1)
		if j < 1 {
			connectionCtx = cc
		}
		idx++
	}
	return connectionCtx
}
