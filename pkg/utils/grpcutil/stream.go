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
	"net"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"github.com/tikv/pd/pkg/errs"
)

// StreamSendDurationCollector measures gRPC stream Send durations and owns the
// lifecycle of HistogramVec children whose target label is derived from peer IPs.
type StreamSendDurationCollector struct {
	hist     *prometheus.HistogramVec
	mu       sync.Mutex
	children map[streamChildKey]int
}

type streamChildKey struct {
	request string
	target  string
}

// NewGRPCStreamSendDuration creates a histogram for measuring gRPC stream
// Send operation durations, using consistent bucket settings across services.
func NewGRPCStreamSendDuration(namespace, subsystem string) *StreamSendDurationCollector {
	return &StreamSendDurationCollector{
		hist: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "grpc_stream_send_duration_seconds",
			Help:      "Bucketed histogram of duration (s) of gRPC stream Send operations.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms ~ 52s
		}, []string{"request", "target"}),
		children: make(map[streamChildKey]int),
	}
}

// Describe implements prometheus.Collector.
func (d *StreamSendDurationCollector) Describe(ch chan<- *prometheus.Desc) {
	d.hist.Describe(ch)
}

// Collect implements prometheus.Collector.
func (d *StreamSendDurationCollector) Collect(ch chan<- prometheus.Metric) {
	d.hist.Collect(ch)
}

func (d *StreamSendDurationCollector) acquire(request, target string) (prometheus.Observer, func()) {
	d.mu.Lock()
	defer d.mu.Unlock()

	key := streamChildKey{request: request, target: target}
	d.children[key]++
	obs := d.hist.WithLabelValues(key.request, key.target)
	return obs, func() { d.release(key) }
}

func (d *StreamSendDurationCollector) release(key streamChildKey) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.children[key]--
	if d.children[key] > 0 {
		return
	}
	delete(d.children, key)
	d.hist.DeleteLabelValues(key.request, key.target)
}

// targetIP extracts the target IP (without port) from a gRPC server stream's peer info.
// The port is stripped to avoid high-cardinality Prometheus labels, since ephemeral
// ports change across reconnections and would create unbounded time series.
// Returns "unknown" if the peer information is unavailable.
func targetIP(stream grpc.ServerStream) string {
	if stream == nil {
		return "unknown"
	}
	p, ok := peer.FromContext(stream.Context())
	if !ok || p.Addr == nil {
		return "unknown"
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return p.Addr.String()
	}
	return host
}

// MetricsStream is a generic gRPC server stream wrapper that automatically
// records Send operation durations via a prometheus observer.
//
// Only Send (and SendAndClose) is measured. Recv is intentionally not timed
// because gRPC Recv blocks until the next client message arrives, so its
// latency mostly reflects the idle interval between client requests (e.g.
// heartbeat periods) rather than any meaningful server-side cost.
type MetricsStream[SendT any, RecvT any] struct {
	grpc.ServerStream
	sendFn  func(SendT) error
	recvFn  func() (RecvT, error)
	sendObs prometheus.Observer
}

// NewMetricsStream creates a MetricsStream wrapping the given gRPC server stream.
// It automatically extracts the target IP from the stream's peer info and combines
// it with requestLabel to create the prometheus observer from collector.
// If collector or stream is nil, no metrics are recorded.
func NewMetricsStream[SendT any, RecvT any](
	stream grpc.ServerStream,
	sendFn func(SendT) error,
	recvFn func() (RecvT, error),
	collector *StreamSendDurationCollector,
	requestLabel string,
) *MetricsStream[SendT, RecvT] {
	ms := &MetricsStream[SendT, RecvT]{
		ServerStream: stream,
		sendFn:       sendFn,
		recvFn:       recvFn,
	}
	if collector != nil && stream != nil {
		var release func()
		ms.sendObs, release = collector.acquire(requestLabel, targetIP(stream))
		context.AfterFunc(stream.Context(), release)
	}
	return ms
}

// Send delegates to the underlying stream's Send and records the duration.
func (s *MetricsStream[SendT, RecvT]) Send(m SendT) error {
	if s.sendObs == nil {
		return s.sendFn(m)
	}
	start := time.Now()
	err := s.sendFn(m)
	s.sendObs.Observe(time.Since(start).Seconds())
	return err
}

// SendAndClose delegates to the underlying stream's SendAndClose and records the duration.
func (s *MetricsStream[SendT, RecvT]) SendAndClose(m SendT) error {
	if s.sendObs == nil {
		return s.sendFn(m)
	}
	start := time.Now()
	err := s.sendFn(m)
	s.sendObs.Observe(time.Since(start).Seconds())
	return err
}

// Recv delegates to the underlying stream's Recv.
func (s *MetricsStream[SendT, RecvT]) Recv() (RecvT, error) {
	if s.recvFn == nil {
		var zero RecvT
		return zero, errs.ErrRecvNotSupported
	}
	return s.recvFn()
}
