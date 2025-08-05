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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/client/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestCancelFunc(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	manager := NewManager[int]()
	url := "test-url"
	manager.Store(ctx, cancel, url, 1)
	re.True(manager.Exist(url))
	manager.GC(func(url string) bool {
		return url == "test-url"
	})
	select {
	case <-ctx.Done():
		re.Equal(context.Canceled, ctx.Err())
	default:
		re.Fail("should not have been called")
	}
}

func TestManager(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	manager := NewManager[int]()

	re.False(manager.Exist("test-url"))
	manager.Store(ctx, cancel, "test-url", 1)
	re.True(manager.Exist("test-url"))

	cctx := manager.RandomlyPick()
	re.Equal("test-url", cctx.StreamURL)
	re.Equal(1, cctx.Stream)
	re.Equal(cctx, manager.GetConnectionCtx("test-url"))

	manager.Store(ctx, cancel, "test-url", 2)
	cctx = manager.RandomlyPick()
	re.Equal("test-url", cctx.StreamURL)
	re.Equal(1, cctx.Stream)
	re.Equal(cctx, manager.GetConnectionCtx("test-url"))

	manager.Store(ctx, cancel, "test-url", 2, true)
	cctx = manager.RandomlyPick()
	re.Equal("test-url", cctx.StreamURL)
	re.Equal(2, cctx.Stream)
	re.Equal(cctx, manager.GetConnectionCtx("test-url"))

	manager.Store(ctx, cancel, "test-another-url", 3)
	pickedCount := make(map[string]int)
	for range 1000 {
		cctx = manager.RandomlyPick()
		pickedCount[cctx.StreamURL]++
	}
	re.NotEmpty(pickedCount["test-url"])
	re.NotEmpty(pickedCount["test-another-url"])
	re.Equal(1000, pickedCount["test-url"]+pickedCount["test-another-url"])

	manager.GC(func(url string) bool {
		return url == "test-url"
	})
	re.False(manager.Exist("test-url"))
	re.Nil(manager.GetConnectionCtx("test-url"))
	re.True(manager.Exist("test-another-url"))
	re.Equal(3, manager.GetConnectionCtx("test-another-url").Stream)

	manager.CleanAllAndStore(ctx, cancel, "test-url", 1)
	re.True(manager.Exist("test-url"))
	re.Equal(1, manager.GetConnectionCtx("test-url").Stream)
	re.False(manager.Exist("test-another-url"))
	re.Nil(manager.GetConnectionCtx("test-another-url"))

	manager.Store(ctx, cancel, "test-another-url", 3)
	manager.CleanAllAndStore(ctx, cancel, "test-unique-url", 4)
	re.True(manager.Exist("test-unique-url"))
	re.Equal(4, manager.GetConnectionCtx("test-unique-url").Stream)
	re.False(manager.Exist("test-url"))
	re.Nil(manager.GetConnectionCtx("test-url"))
	re.False(manager.Exist("test-another-url"))
	re.Nil(manager.GetConnectionCtx("test-another-url"))

	manager.Release("test-unique-url")
	re.False(manager.Exist("test-unique-url"))
	re.Nil(manager.GetConnectionCtx("test-unique-url"))

	for i := range 1000 {
		manager.Store(ctx, cancel, fmt.Sprintf("test-url-%d", i), i)
	}
	re.Len(manager.connectionCtxs, 1000)
	manager.ReleaseAll()
	re.Empty(manager.connectionCtxs)
}
