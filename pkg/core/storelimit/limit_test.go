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

package storelimit

import (
	"container/list"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/core/constant"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestStoreLimit(t *testing.T) {
	re := require.New(t)
	rate := int64(15)
	limit := NewStoreRateLimit(float64(rate)).(*StoreRateLimit)
	re.Equal(float64(15), limit.Rate(AddPeer))
	re.True(limit.Available(influence*rate, AddPeer, constant.Low))
	re.True(limit.Take(influence*rate, AddPeer, constant.Low))
	re.False(limit.Take(influence, AddPeer, constant.Low))

	limit.Reset(float64(rate), AddPeer)
	re.False(limit.Available(influence, AddPeer, constant.Low))
	re.False(limit.Take(influence, AddPeer, constant.Low))

	limit.Reset(0, AddPeer)
	re.True(limit.Available(influence, AddPeer, constant.Low))
	re.True(limit.Take(influence, AddPeer, constant.Low))
}

func TestSlidingWindow(t *testing.T) {
	re := require.New(t)
	capacity := int64(defaultWindowSize)
	s := NewSlidingWindows()
	re.Len(s.windows, int(constant.PriorityLevelLen))
	// capacity:[10, 10, 10, 10]
	for i, v := range s.windows {
		cap := capacity >> i
		if cap < minSnapSize {
			cap = minSnapSize
		}
		re.Equal(v.capacity, cap)
	}
	// case 0: test core.Low level
	re.True(s.Available(capacity, SendSnapshot, constant.Low))
	re.True(s.Take(capacity, SendSnapshot, constant.Low))
	re.False(s.Available(capacity, SendSnapshot, constant.Low))
	s.Ack(capacity, SendSnapshot)
	re.True(s.Available(capacity, SendSnapshot, constant.Low))

	// case 1: it will occupy the normal window size not the core.High window.
	re.True(s.Take(capacity, SendSnapshot, constant.High))
	re.Equal([]int64{capacity, 0, 0, 0}, s.GetUsed())
	re.EqualValues(0, s.windows[constant.High].getUsed())
	s.Ack(capacity, SendSnapshot)
	re.Equal([]int64{0, 0, 0, 0}, s.GetUsed())

	// case 2: it will occupy the core.High window size if the normal window is full.
	capacity = 2000
	s.set(float64(capacity), SendSnapshot)
	re.True(s.Take(capacity-minSnapSize, SendSnapshot, constant.Low))
	re.True(s.Take(capacity-minSnapSize, SendSnapshot, constant.Low))
	re.False(s.Take(capacity, SendSnapshot, constant.Low))
	re.True(s.Take(capacity-minSnapSize, SendSnapshot, constant.Medium))
	re.False(s.Take(capacity-minSnapSize, SendSnapshot, constant.Medium))
	re.Equal([]int64{capacity + capacity - minSnapSize*2, capacity - minSnapSize, 0, 0}, s.GetUsed())
	s.Ack(capacity-minSnapSize, SendSnapshot)
	s.Ack(capacity-minSnapSize, SendSnapshot)
	re.Equal([]int64{capacity - minSnapSize, 0, 0, 0}, s.GetUsed())

	// case 3: skip the type is not the SendSnapshot
	for range 10 {
		re.True(s.Take(capacity, AddPeer, constant.Low))
	}
}

func TestWindow(t *testing.T) {
	re := require.New(t)
	capacity := int64(100 * 10)
	s := newWindow(capacity)

	//case1: token maybe greater than the capacity.
	token := capacity + 10
	re.True(s.take(token))
	re.False(s.take(token))
	re.EqualValues(0, s.ack(token))
	re.True(s.take(token))
	re.EqualValues(0, s.ack(token))
	re.Equal(s.ack(token), token)
	re.EqualValues(0, s.getUsed())

	// case2: the capacity of the window must greater than the minSnapSize.
	s.reset(minSnapSize - 1)
	re.EqualValues(minSnapSize, s.capacity)
	re.True(s.take(minSnapSize))
	re.EqualValues(minSnapSize, s.ack(minSnapSize*2))
	re.EqualValues(0, s.getUsed())
}

func TestFeedback(t *testing.T) {
	re := require.New(t)
	s := NewSlidingWindows()
	type SnapshotStats struct {
		expectCost int64
		remaining  int64
		size       int64
		start      int64
	}
	// region size is 10GB, snapshot write limit is 100MB/s and the snapshot concurrency is 3.
	// the best strategy is that the tikv executing queue equals the wait.
	const regionSize, limit, wait = int64(10000), int64(100), int64(4)
	iter := 100

	// receive the operator
	queue := list.List{}
	interval := time.Microsecond * 100
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// create one region operator
	generateOp := func(tick int64) {
		if s.Available(regionSize, SendSnapshot, constant.Low) && iter > 0 {
			iter--
			size := regionSize - rand.Int64N(regionSize/10)
			stats := &SnapshotStats{
				expectCost: size / limit,
				remaining:  size,
				size:       size,
				start:      tick,
			}
			s.Take(size, SendSnapshot, constant.Low)
			queue.PushBack(stats)
		}
	}

	// tick is the time that the snapshot has been executed.
	tick := int64(0)
	for range ticker.C {
		tick++
		generateOp(tick)
		first := queue.Front()
		if first == nil {
			continue
		}
		stats := first.Value.(*SnapshotStats)
		if stats.remaining > 0 {
			stats.remaining -= limit
			continue
		}
		cost := tick - stats.start
		exec := stats.expectCost
		if exec < 5 {
			exec = 5
		}
		err := exec*wait - cost
		queue.Remove(first)
		s.Feedback(float64(err))
		if iter <= 0 {
			re.Greater(float64(s.GetCap()), float64(regionSize*(wait-2)))
			re.Less(float64(s.GetCap()), float64(regionSize*wait))
			return
		}
		s.Ack(stats.size, SendSnapshot)
	}
}
