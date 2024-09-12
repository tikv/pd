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

package pd

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap/zapcore"
)

type mockTSOServiceProvider struct {
	option *option
}

func newMockTSOServiceProvider(option *option) *mockTSOServiceProvider {
	return &mockTSOServiceProvider{
		option: option,
	}
}

func (m *mockTSOServiceProvider) getOption() *option {
	return m.option
}

func (m *mockTSOServiceProvider) getServiceDiscovery() ServiceDiscovery {
	return NewMockPDServiceDiscovery([]string{mockStreamURL}, nil)
}

func (m *mockTSOServiceProvider) updateConnectionCtxs(ctx context.Context, _dc string, connectionCtxs *sync.Map) bool {
	_, ok := connectionCtxs.Load(mockStreamURL)
	if ok {
		return true
	}
	ctx, cancel := context.WithCancel(ctx)
	stream := &tsoStream{
		serverURL: mockStreamURL,
		stream:    newMockTSOStreamImpl(ctx, true),
	}
	connectionCtxs.LoadOrStore(mockStreamURL, &tsoConnectionContext{ctx, cancel, mockStreamURL, stream})
	return true
}

func BenchmarkTSODispatcherHandleRequests(b *testing.B) {
	log.SetLevel(zapcore.FatalLevel)

	ctx := context.Background()

	reqPool := &sync.Pool{
		New: func() any {
			return &tsoRequest{
				done:       make(chan error, 1),
				physical:   0,
				logical:    0,
				dcLocation: globalDCLocation,
			}
		},
	}
	getReq := func() *tsoRequest {
		req := reqPool.Get().(*tsoRequest)
		req.clientCtx = ctx
		req.requestCtx = ctx
		req.physical = 0
		req.logical = 0
		req.start = time.Now()
		req.pool = reqPool
		return req
	}

	dispatcher := newTSODispatcher(ctx, globalDCLocation, defaultMaxTSOBatchSize, newMockTSOServiceProvider(newOption()))
	var wg sync.WaitGroup
	wg.Add(1)

	go dispatcher.handleDispatcher(&wg)
	defer func() {
		dispatcher.close()
		wg.Wait()
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := getReq()
		dispatcher.push(req)
		_, _, err := req.Wait()
		if err != nil {
			panic(fmt.Sprintf("unexpected error from tsoReq: %+v", err))
		}
	}
	// Don't count the time cost in `defer`
	b.StopTimer()
}
