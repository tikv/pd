// Copyright 2026 TiKV Project Authors.
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

package grpcutil

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/testutil"
)

// fakeServerStream implements grpc.ServerStream for testing.
type fakeServerStream struct {
	ctx context.Context
}

const testTarget = "10.0.1.5"

func (*fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (*fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (*fakeServerStream) SetTrailer(metadata.MD)       {}
func (f *fakeServerStream) Context() context.Context   { return f.ctx }
func (*fakeServerStream) SendMsg(any) error            { return nil }
func (*fakeServerStream) RecvMsg(any) error            { return nil }

func newFakeStreamWithPeer() *fakeServerStream {
	return newFakeStreamWithPeerContext(context.Background(), testTarget)
}

func newFakeStreamWithPeerContext(ctx context.Context, ip string) *fakeServerStream {
	ctx = peer.NewContext(ctx, &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP(ip), Port: 34567},
	})
	return &fakeServerStream{ctx: ctx}
}

func newTestStreamMetrics(t *testing.T) (*prometheus.Registry, *StreamSendDurationCollector) {
	t.Helper()
	reg := prometheus.NewRegistry()
	h := NewGRPCStreamSendDuration("test", "stream")
	require.NoError(t, reg.Register(h))
	return reg, h
}

func newTestMetricsStream(
	ctx context.Context,
	hist *StreamSendDurationCollector,
	request, target string,
) *MetricsStream[string, string] {
	return NewMetricsStream[string, string](
		newFakeStreamWithPeerContext(ctx, target),
		func(string) error { return nil },
		nil,
		hist, request,
	)
}

func streamMetricCount(reg prometheus.Gatherer) (int, error) {
	mfs, err := reg.Gather()
	if err != nil {
		return 0, err
	}
	if len(mfs) == 0 {
		return 0, nil
	}
	return len(mfs[0].GetMetric()), nil
}

func streamMetricSampleCount(reg prometheus.Gatherer, request string) (uint64, bool, error) {
	mfs, err := reg.Gather()
	if err != nil {
		return 0, false, err
	}
	for _, mf := range mfs {
		for _, metric := range mf.GetMetric() {
			var metricRequest, metricTarget string
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "request":
					metricRequest = label.GetValue()
				case "target":
					metricTarget = label.GetValue()
				}
			}
			if metricRequest == request && metricTarget == testTarget {
				return metric.GetHistogram().GetSampleCount(), true, nil
			}
		}
	}
	return 0, false, nil
}

func waitForNoStreamMetrics(t *testing.T, reg prometheus.Gatherer) {
	t.Helper()
	var gatherErr error
	testutil.Eventually(
		require.New(t),
		func() bool {
			var count int
			count, err := streamMetricCount(reg)
			if err != nil {
				gatherErr = err
				return true
			}
			return count == 0
		},
		testutil.WithWaitFor(time.Second),
		testutil.WithTickInterval(time.Millisecond),
	)
	require.NoError(t, gatherErr)
}

func TestMetricsStreamLifecycle(t *testing.T) {
	re := require.New(t)
	reg, h := newTestStreamMetrics(t)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	stream1 := newTestMetricsStream(ctx1, h, "my-rpc", testTarget)
	stream2 := newTestMetricsStream(ctx2, h, "my-rpc", testTarget)
	re.NoError(stream1.Send("first"))
	re.NoError(stream2.Send("second"))
	metricCount, err := streamMetricCount(reg)
	re.NoError(err)
	re.Equal(1, metricCount)

	key := streamChildKey{request: "my-rpc", target: testTarget}
	h.mu.Lock()
	refs := h.children[key]
	h.mu.Unlock()
	re.Equal(2, refs)

	cancel1()
	testutil.Eventually(
		re,
		func() bool {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.children[key] == 1
		},
		testutil.WithWaitFor(time.Second),
		testutil.WithTickInterval(time.Millisecond),
	)
	metricCount, err = streamMetricCount(reg)
	re.NoError(err)
	re.Equal(1, metricCount)
	re.NoError(stream2.Send("still-active"))
	count, ok, err := streamMetricSampleCount(reg, "my-rpc")
	re.NoError(err)
	re.True(ok)
	re.Equal(uint64(3), count)

	cancel2()
	waitForNoStreamMetrics(t, reg)
}

func TestMetricsStreamReconnect(t *testing.T) {
	re := require.New(t)
	reg, h := newTestStreamMetrics(t)

	ctx1, cancel1 := context.WithCancel(context.Background())
	stream1 := newTestMetricsStream(ctx1, h, "my-rpc", testTarget)
	re.NoError(stream1.Send("first"))
	re.NoError(stream1.Send("second"))
	count, ok, err := streamMetricSampleCount(reg, "my-rpc")
	re.NoError(err)
	re.True(ok)
	re.Equal(uint64(2), count)

	cancel1()
	waitForNoStreamMetrics(t, reg)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	stream2 := newTestMetricsStream(ctx2, h, "my-rpc", testTarget)
	re.NoError(stream2.Send("reconnected"))
	count, ok, err = streamMetricSampleCount(reg, "my-rpc")
	re.NoError(err)
	re.True(ok)
	re.Equal(uint64(1), count)

	cancel2()
	waitForNoStreamMetrics(t, reg)
}

func TestMetricsStreamLabelIsolation(t *testing.T) {
	re := require.New(t)
	reg, h := newTestStreamMetrics(t)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	stream1 := newTestMetricsStream(ctx1, h, "first-rpc", testTarget)
	stream2 := newTestMetricsStream(ctx2, h, "second-rpc", testTarget)
	re.NoError(stream1.Send("first"))
	re.NoError(stream2.Send("second"))
	metricCount, err := streamMetricCount(reg)
	re.NoError(err)
	re.Equal(2, metricCount)

	cancel1()
	var gatherErr error
	testutil.Eventually(
		re,
		func() bool {
			_, firstExists, err := streamMetricSampleCount(reg, "first-rpc")
			if err != nil {
				gatherErr = err
				return true
			}
			secondCount, secondExists, err := streamMetricSampleCount(reg, "second-rpc")
			if err != nil {
				gatherErr = err
				return true
			}
			return !firstExists && secondExists && secondCount == 1
		},
		testutil.WithWaitFor(time.Second),
		testutil.WithTickInterval(time.Millisecond),
	)
	re.NoError(gatherErr)

	cancel2()
	waitForNoStreamMetrics(t, reg)
}

func TestMetricsStreamAlreadyCanceled(t *testing.T) {
	reg, h := newTestStreamMetrics(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	newTestMetricsStream(ctx, h, "my-rpc", testTarget)

	waitForNoStreamMetrics(t, reg)
}

func TestMetricsStreamConcurrentLifecycle(t *testing.T) {
	reg, h := newTestStreamMetrics(t)

	const streamCount = 128
	var wg sync.WaitGroup
	wg.Add(streamCount)
	for i := range streamCount {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			newTestMetricsStream(
				ctx,
				h,
				fmt.Sprintf("rpc-%d", i%4),
				fmt.Sprintf("10.0.1.%d", i%8+1),
			)
			cancel()
		}()
	}
	wg.Wait()

	waitForNoStreamMetrics(t, reg)
}

func TestMetricsStream(t *testing.T) {
	t.Run("SendAndRecv", func(t *testing.T) {
		re := require.New(t)
		reg := prometheus.NewRegistry()
		h := NewGRPCStreamSendDuration("test", "stream")
		re.NoError(reg.Register(h))

		var sent []string
		fake := newFakeStreamWithPeer()
		s := NewMetricsStream[string, string](
			fake,
			func(m string) error { sent = append(sent, m); return nil },
			func() (string, error) { return "data", nil },
			h, "my-rpc",
		)

		re.NoError(s.Send("a"))
		re.NoError(s.Send("b"))
		re.NoError(s.SendAndClose("c"))
		re.Equal([]string{"a", "b", "c"}, sent)

		v, err := s.Recv()
		re.NoError(err)
		re.Equal("data", v)

		// Verify histogram: 3 observations (2 Send + 1 SendAndClose).
		mfs, err := reg.Gather()
		re.NoError(err)
		re.Len(mfs, 1)
		re.Equal("test_stream_grpc_stream_send_duration_seconds", mfs[0].GetName())
		re.Equal(uint64(3), mfs[0].GetMetric()[0].GetHistogram().GetSampleCount())
	})

	t.Run("ErrorPropagation", func(t *testing.T) {
		re := require.New(t)
		sendErr := errors.New("send failed")
		recvErr := errors.New("recv failed")
		fake := newFakeStreamWithPeer()

		s := NewMetricsStream[string, string](
			fake,
			func(string) error { return sendErr },
			func() (string, error) { return "", recvErr },
			nil, "",
		)
		re.ErrorIs(s.Send("x"), sendErr)
		re.ErrorIs(s.SendAndClose("x"), sendErr)
		_, err := s.Recv()
		re.ErrorIs(err, recvErr)
	})

	t.Run("NilRecvFn", func(t *testing.T) {
		re := require.New(t)
		s := NewMetricsStream[string, string](
			nil,
			func(string) error { return nil },
			nil,
			nil, "",
		)
		_, err := s.Recv()
		re.ErrorIs(err, errs.ErrRecvNotSupported)
	})

	t.Run("NilObserver", func(t *testing.T) {
		re := require.New(t)
		s := NewMetricsStream[string, string](
			nil,
			func(string) error { return nil },
			func() (string, error) { return "ok", nil },
			nil, "",
		)
		re.NoError(s.Send("no-obs"))
		re.NoError(s.SendAndClose("no-obs"))
		v, err := s.Recv()
		re.NoError(err)
		re.Equal("ok", v)
	})

	t.Run("NilStream", func(t *testing.T) {
		re := require.New(t)
		reg, h := newTestStreamMetrics(t)
		s := NewMetricsStream[string, string](
			nil,
			func(string) error { return nil },
			nil,
			h, "my-rpc",
		)
		re.NoError(s.Send("no-stream"))
		metricCount, err := streamMetricCount(reg)
		re.NoError(err)
		re.Equal(0, metricCount)
	})
}

func TestTargetIP(t *testing.T) {
	re := require.New(t)

	// nil stream returns "unknown".
	re.Equal("unknown", targetIP(nil))

	// No peer info in context returns "unknown".
	s := &fakeServerStream{ctx: context.Background()}
	re.Equal("unknown", targetIP(s))

	// With peer addr, returns IP only (without port).
	s = newFakeStreamWithPeer()
	re.Equal(testTarget, targetIP(s))
}
