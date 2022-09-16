package storelimit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlidingWindow(t *testing.T) {
	t.Parallel()
	re := assert.New(t)
	capacity := int64(10)
	s := NewSlidingWindows(capacity)
	re.Len(s.windows, int(MaxPriority))
	// capacity:[10, 10]
	for i, v := range s.windows {
		cap := capacity >> i
		if cap < snapSize {
			cap = snapSize
		}
		re.EqualValues(v.capacity, cap)
	}
	// case 1: it will occupy the normal window size not the high window.
	re.True(s.Take(capacity, HighPriority))
	re.EqualValues(s.GetUsed(), capacity)
	re.EqualValues(s.windows[NormalPriority].getUsed(), capacity)
	s.Ack(capacity)
	re.EqualValues(s.GetUsed(), 0)

	// case 2: it will occupy the high window size if the normal window is full.
	re.True(s.Take(capacity, NormalPriority))
	re.False(s.Take(capacity, NormalPriority))
	re.True(s.Take(capacity-100, HighPriority))
	re.False(s.Take(capacity-100, HighPriority))
	re.EqualValues(s.GetUsed(), capacity+capacity-100)
	s.Ack(capacity)
	re.Equal(s.GetUsed(), capacity-100)
}

func TestWindow(t *testing.T) {
	t.Parallel()
	re := assert.New(t)
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

	// case2: the capacity of the window must greater than the snapSize.
	s.reset(snapSize - 1)
	re.EqualValues(s.capacity, snapSize)
	re.True(s.take(snapSize))
	re.EqualValues(s.ack(snapSize*2), snapSize)
	re.EqualValues(s.getUsed(), 0)
}
