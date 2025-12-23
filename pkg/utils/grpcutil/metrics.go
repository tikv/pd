// Copyright 2025 TiKV Project Authors.
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
	"sync"
	"sync/atomic"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	grpcClientMetricsOnce    sync.Once
	grpcClientMetricsEnabled atomic.Bool
	grpcClientMetrics        *grpcprometheus.ClientMetrics
)

// EnableGRPCClientMetrics enables grpc_client_* metrics for all outgoing gRPC
// requests created by this process.
//
// It's safe to call multiple times.
func EnableGRPCClientMetrics() {
	grpcClientMetricsOnce.Do(func() {
		// Prefer using the package-level DefaultClientMetrics to avoid registering
		// multiple instances with the same metric names in a single process.
		grpcprometheus.EnableClientHandlingTimeHistogram(
			grpcprometheus.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.5, 1, 2.5, 5, 10}),
		)
		m := grpcprometheus.DefaultClientMetrics
		err := prometheus.Register(m)
		if err != nil {
			// The metrics may have been registered by another component in the same process.
			// If so, reuse the existing one to avoid panicking or disabling metrics silently.
			if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
				if existing, ok := are.ExistingCollector.(*grpcprometheus.ClientMetrics); ok {
					grpcClientMetrics = existing
					grpcClientMetricsEnabled.Store(true)
					return
				}
			}
			// The default registry may already contain collectors with these descriptors.
			// In that case, we still enable interceptors against DefaultClientMetrics.
			log.Warn("failed to register grpc client metrics, using default client metrics anyway", zap.Error(err))
		}
		grpcClientMetrics = m
		grpcClientMetricsEnabled.Store(true)
	})
}

// ClientMetricsDialOptions returns grpc.DialOptions that enable grpc_client_* metrics
// for all outgoing gRPC requests created by this process.
//
// It uses chain interceptors so callers can safely append additional interceptors
// without overriding these metrics interceptors.
func ClientMetricsDialOptions() []grpc.DialOption {
	if !grpcClientMetricsEnabled.Load() {
		return nil
	}
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(grpcClientMetrics.UnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(grpcClientMetrics.StreamClientInterceptor()),
	}
}
