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
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMetricsStream(t *testing.T) {
	re := require.New(t)

	reg := prometheus.NewRegistry()
	h := NewGRPCStreamSendDuration("test", "stream")
	re.NoError(reg.Register(h))

	sendErr := errors.New("send failed")
	recvErr := errors.New("recv failed")
	var sent []string
	s := NewMetricsStream[string, string](
		nil,
		func(m string) error { sent = append(sent, m); return nil },
		func() (string, error) { return "data", nil },
		h.WithLabelValues("my-rpc"),
	)

	// Send records duration and delegates correctly.
	re.NoError(s.Send("a"))
	re.NoError(s.Send("b"))
	re.Equal([]string{"a", "b"}, sent)

	// SendAndClose shares the same observer.
	re.NoError(s.SendAndClose("c"))
	re.Equal([]string{"a", "b", "c"}, sent)

	// Recv simply delegates without timing.
	v, err := s.Recv()
	re.NoError(err)
	re.Equal("data", v)

	// Send error is propagated; duration is still recorded.
	sErr := NewMetricsStream[string, string](
		nil,
		func(string) error { return sendErr },
		nil,
		h.WithLabelValues("my-rpc"),
	)
	re.ErrorIs(sErr.Send("x"), sendErr)

	// Recv error is propagated.
	sRecvErr := NewMetricsStream[string, string](
		nil,
		nil,
		func() (string, error) { return "", recvErr },
		nil,
	)
	_, err = sRecvErr.Recv()
	re.ErrorIs(err, recvErr)

	// Nil observer: Send/SendAndClose still work without panic.
	sNil := NewMetricsStream[string, string](
		nil,
		func(string) error { return nil },
		nil,
		nil,
	)
	re.NoError(sNil.Send("no-obs"))
	re.NoError(sNil.SendAndClose("no-obs"))

	// Verify prometheus histogram: 4 observations (2 Send + 1 SendAndClose + 1 error Send).
	mfs, err := reg.Gather()
	re.NoError(err)
	re.Len(mfs, 1)
	re.Equal("test_stream_grpc_stream_send_duration_seconds", mfs[0].GetName())
	re.Equal(uint64(4), mfs[0].GetMetric()[0].GetHistogram().GetSampleCount())
}
