// Copyright 2022 TiKV Project Authors.
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

package id

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

const (
	rootPath   = "/pd"
	leaderPath = "/pd/leader"
	allocPath  = "alloc_id"
	label      = "idalloc"
	memberVal  = "member"
)

// TestMultipleAllocator tests situation where multiple allocators that
// share rootPath and member val can update their id concurrently.
func TestMultipleAllocator(t *testing.T) {
	re := require.New(t)
	cfg := etcdutil.NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	defer func() {
		etcd.Close()
	}()
	re.NoError(err)

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)

	<-etcd.Server.ReadyNotify()

	// put memberValue to leaderPath to simulate an election success.
	_, err = client.Put(context.Background(), leaderPath, memberVal)
	re.NoError(err)

	wg := sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		iStr := strconv.Itoa(i)
		wg.Add(1)
		// allocators share rootPath and memberVal, but have different allocPaths and labels
		allocator := NewAllocator(client, rootPath, allocPath+iStr, label+iStr, memberVal)
		go func(re *require.Assertions, allocator Allocator) {
			defer wg.Done()
			testAllocator(re, allocator)
		}(re, allocator)
	}
	wg.Wait()
}

// testAllocator keep updated given allocator and check if values are expected
func testAllocator(re *require.Assertions, allocator Allocator) {
	startID, err := allocator.Alloc()
	re.NoError(err)
	for i := startID + 1; i < startID+allocStep*2; i++ {
		id, err := allocator.Alloc()
		re.NoError(err)
		re.Equal(i, id)
	}
}
