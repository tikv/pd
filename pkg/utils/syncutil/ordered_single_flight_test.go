package syncutil

import (
	"context"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestOrderedSingleFlight(t *testing.T) {
	re := require.New(t)

	ctx := context.Background()
	s := NewOrderedSingleFlight[int]()

	res, err := s.Do(ctx, func() (int, error) {
		return 1, nil
	})
	re.NoError(err)
	re.Equal(1, res)

	res, err = s.Do(ctx, func() (int, error) {
		return 2, errors.New("err")
	})
	re.Error(err)
	re.Equal(2, res)

	const concurrency = 5

	inCh := make(chan int, 2)
	outCh := make(chan int, concurrency+1)

	f := func() (int, error) {
		return <-inCh, nil
	}

	// Start the first call
	go func() {
		res, err := s.Do(ctx, f)
		re.NoError(err)
		outCh <- res
	}()

	select {
	case <-outCh:
		re.Fail("received result when the execution is expected still running")
	case <-time.After(100 * time.Millisecond):
	}

	// Start some concurrent invocations. As there's already an existing execution, the following invocations will enter
	// the next batch and return the second result.
	for i := 0; i < concurrency; i++ {
		i2 := i
		go func() {
			labels := pprof.Labels("worker", strconv.FormatInt(int64(i2), 10))
			pprof.Do(context.Background(), labels, func(context.Context) {
				res, err := s.Do(ctx, func() (int, error) {
					return <-inCh, nil
				})
				re.NoError(err)
				outCh <- res
			})
		}()
	}

	select {
	case <-outCh:
		re.Fail("received result when the execution is expected still running")
	case <-time.After(100 * time.Millisecond):
	}

	inCh <- 101
	// The first call finishes.
	select {
	case res := <-outCh:
		re.Equal(101, res)
	case <-time.After(time.Second):
		re.Fail("result not received")
	}
	// But the other calls are still blocked.
	select {
	case <-outCh:
		re.Fail("received result when the execution is expected still running")
	case <-time.After(100 * time.Millisecond):
	}

	inCh <- 102
	// The remaining calls finishes.
	for i := 0; i < concurrency; i++ {
		select {
		case res := <-outCh:
			re.Equal(102, res)
		case <-time.After(time.Second):
			re.Fail("result not received")
		}
	}

	// No abnormal internal state is left.
	re.Nil(s.pendingTask)
	res, err = s.Do(ctx, func() (int, error) {
		return 200, nil
	})
	re.NoError(err)
	re.Equal(200, res)
}
