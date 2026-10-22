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
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestCancelFunc(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	manager := NewManager[int]()
	url := "test-url"
	re.True(manager.Store(ctx, cancel, url, 1))
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
	re.True(manager.Store(ctx, cancel, "test-url", 1))
	re.True(manager.Exist("test-url"))

	cctx := manager.RandomlyPick()
	re.Equal("test-url", cctx.StreamURL)
	re.Equal(1, cctx.Stream)
	re.Equal(cctx, manager.GetConnectionCtx("test-url"))

	re.False(manager.Store(ctx, cancel, "test-url", 2))
	cctx = manager.RandomlyPick()
	re.Equal("test-url", cctx.StreamURL)
	re.Equal(1, cctx.Stream)
	re.Equal(cctx, manager.GetConnectionCtx("test-url"))

	re.True(manager.Store(ctx, cancel, "test-url", 2, true))
	cctx = manager.RandomlyPick()
	re.Equal("test-url", cctx.StreamURL)
	re.Equal(2, cctx.Stream)
	re.Equal(cctx, manager.GetConnectionCtx("test-url"))

	re.True(manager.Store(ctx, cancel, "test-another-url", 3))
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

	re.True(manager.CleanAllAndStore(ctx, cancel, "test-url", 1))
	re.True(manager.Exist("test-url"))
	re.False(manager.CleanAllAndStore(ctx, cancel, "test-url", 2))
	re.Equal(1, manager.GetConnectionCtx("test-url").Stream)
	re.False(manager.Exist("test-another-url"))
	re.Nil(manager.GetConnectionCtx("test-another-url"))

	manager.Store(ctx, cancel, "test-another-url", 3)
	re.True(manager.CleanAllAndStore(ctx, cancel, "test-unique-url", 4))
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

func TestReplaceHook(t *testing.T) {
	re := require.New(t)
	var replaced []int
	manager := NewManagerWithReplaceHook(func(stream int) { replaced = append(replaced, stream) })
	store := func(url string, stream int, overwrite bool) context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		re.True(manager.Store(ctx, cancel, url, stream, overwrite))
		return ctx
	}
	// GC, CleanAllAndStore and an overwriting Store replace connections: the hook runs
	// before they are canceled.
	ctx1 := store("url1", 1, false)
	ctx2 := store("url1", 2, true)
	re.Error(ctx1.Err())
	ctx3 := store("url3", 3, false)
	manager.GC(func(url string) bool { return url == "url3" })
	re.Error(ctx3.Err())
	ctx4, cancel4 := context.WithCancel(context.Background())
	re.True(manager.CleanAllAndStore(ctx4, cancel4, "url4", 4))
	re.Error(ctx2.Err())
	re.Equal([]int{1, 3, 2}, replaced)
	// Release and ReleaseAll do not replace connections.
	store("url5", 5, false)
	manager.Release("url5")
	manager.ReleaseAll()
	re.Error(ctx4.Err())
	re.Equal([]int{1, 3, 2}, replaced)
}
