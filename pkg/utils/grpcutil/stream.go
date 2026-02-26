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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

// MetricsStream is a generic gRPC server stream wrapper that automatically
// records Send/Recv operation durations via prometheus observers.
type MetricsStream[SendT any, RecvT any] struct {
	grpc.ServerStream
	SendFn  func(SendT) error
	RecvFn  func() (RecvT, error)
	SendObs prometheus.Observer
	RecvObs prometheus.Observer
}

// Send delegates to the underlying stream's Send and records the duration.
func (s *MetricsStream[SendT, RecvT]) Send(m SendT) error {
	if s.SendObs == nil {
		return s.SendFn(m)
	}
	start := time.Now()
	err := s.SendFn(m)
	s.SendObs.Observe(time.Since(start).Seconds())
	return err
}

// SendAndClose delegates to the underlying stream's SendAndClose and records the duration.
func (s *MetricsStream[SendT, RecvT]) SendAndClose(m SendT) error {
	if s.SendObs == nil {
		return s.SendFn(m)
	}
	start := time.Now()
	err := s.SendFn(m)
	s.SendObs.Observe(time.Since(start).Seconds())
	return err
}

// Recv delegates to the underlying stream's Recv and records the duration.
func (s *MetricsStream[SendT, RecvT]) Recv() (RecvT, error) {
	if s.RecvObs == nil {
		return s.RecvFn()
	}
	start := time.Now()
	result, err := s.RecvFn()
	s.RecvObs.Observe(time.Since(start).Seconds())
	return result, err
}
