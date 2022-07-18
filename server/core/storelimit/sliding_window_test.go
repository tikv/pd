package storelimit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SlidingWindows(t *testing.T) {
	t.Parallel()
	capacity := int64(100 * 10)
	re := assert.New(t)
	s := NewSlidingWindows(capacity)
	re.True(s.Available(capacity))
	re.False(s.Available(capacity + 1))
	re.True(s.Take(capacity))
	re.False(s.Take(capacity))
	s.Ack(capacity)
	re.True(s.Take(capacity))
	re.Equal(capacity, s.used)
}
