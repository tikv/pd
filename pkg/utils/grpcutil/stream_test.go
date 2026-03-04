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
	"net"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/tikv/pd/pkg/errs"
)

// fakeServerStream implements grpc.ServerStream for testing.
type fakeServerStream struct {
	ctx context.Context
}

func (*fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (*fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (*fakeServerStream) SetTrailer(metadata.MD)       {}
func (f *fakeServerStream) Context() context.Context   { return f.ctx }
func (*fakeServerStream) SendMsg(any) error            { return nil }
func (*fakeServerStream) RecvMsg(any) error            { return nil }

func newFakeStreamWithPeer() *fakeServerStream {
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("10.0.1.5"), Port: 34567},
	})
	return &fakeServerStream{ctx: ctx}
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
}

func TestClientIP(t *testing.T) {
	re := require.New(t)

	// nil stream returns "unknown".
	re.Equal("unknown", clientIP(nil))

	// No peer info in context returns "unknown".
	s := &fakeServerStream{ctx: context.Background()}
	re.Equal("unknown", clientIP(s))

	// With peer addr, returns IP only (without port).
	s = newFakeStreamWithPeer()
	re.Equal("10.0.1.5", clientIP(s))
}
