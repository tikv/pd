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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreLimit(t *testing.T) {
	re := require.New(t)
	rate := int64(15)
	limit := NewStoreRateLimit(float64(rate))
	re.True(limit.Available(influence*rate, AddPeer, Low))
	re.True(limit.Take(influence*rate, AddPeer, Low))
	re.False(limit.Take(influence, AddPeer, Low))

	limit.Reset(float64(rate), AddPeer)
	re.False(limit.Available(influence, AddPeer, Low))
	re.False(limit.Take(influence, AddPeer, Low))

	limit.Reset(0, AddPeer)
	re.True(limit.Available(influence, AddPeer, Low))
	re.True(limit.Take(influence, AddPeer, Low))
}

func TestSlidingWindow(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	capacity := int64(10)
	s := NewSlidingWindows(float64(capacity))
	re.Len(s.windows, int(priorityLevelLen))
	// capacity:[10, 10, 10, 10]
	for i, v := range s.windows {
		cap := capacity >> i
		if cap < minSnapSize {
			cap = minSnapSize
		}
		re.EqualValues(v.capacity, cap)
	}
	// case 0: test low level
	re.True(s.Available(capacity, AddPeer, Low))
	re.True(s.Take(capacity, AddPeer, Low))
	re.False(s.Available(capacity, AddPeer, Low))
	s.Ack(capacity)
	re.True(s.Available(capacity, AddPeer, Low))

	// case 1: it will occupy the normal window size not the high window.
	re.True(s.Take(capacity, AddPeer, High))
	re.EqualValues(capacity, s.GetUsed())
	re.EqualValues(0, s.windows[High].getUsed())
	s.Ack(capacity)
	re.EqualValues(s.GetUsed(), 0)

	// case 2: it will occupy the high window size if the normal window is full.
	capacity = 1000
	s.Reset(float64(capacity), AddPeer)
	re.True(s.Take(capacity, AddPeer, Low))
	re.False(s.Take(capacity, AddPeer, Low))
	re.True(s.Take(capacity-100, AddPeer, Medium))
	re.False(s.Take(capacity-100, AddPeer, Medium))
	re.EqualValues(s.GetUsed(), capacity+capacity-100)
	s.Ack(capacity)
	re.Equal(s.GetUsed(), capacity-100)
}

func TestWindow(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	capacity := int64(100 * 10)
	s := newWindow(capacity)

	//case1: token maybe greater than the capacity.
	token := capacity + 10
	re.True(s.take(token))
	re.False(s.take(token))
	re.EqualValues(s.ack(token), 0)
	re.True(s.take(token))
	re.EqualValues(s.ack(token), 0)
	re.Equal(s.ack(token), token)
	re.EqualValues(s.getUsed(), 0)

	// case2: the capacity of the window must greater than the minSnapSize.
	s.reset(minSnapSize - 1)
	re.EqualValues(s.capacity, minSnapSize)
	re.True(s.take(minSnapSize))
	re.EqualValues(s.ack(minSnapSize*2), minSnapSize)
	re.EqualValues(s.getUsed(), 0)
}
