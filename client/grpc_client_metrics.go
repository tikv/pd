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

package pd

import (
	"sync"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
)

var (
	grpcClientMetricsOnce sync.Once
)

func initAndRegisterGRPCClientMetrics() *grpc_prometheus.ClientMetrics {
	grpcClientMetricsOnce.Do(func() {
		// NOTE: go-grpc-prometheus already registers the default client counters to the
		// default Prometheus registry in its init(). We just enable the handling-time
		// histogram (also on the default registry) with our desired buckets.
		grpc_prometheus.EnableClientHandlingTimeHistogram(
			grpc_prometheus.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.5, 1, 2.5, 5, 10}),
		)
	})

	return grpc_prometheus.DefaultClientMetrics
}

func grpcClientMetricsDialOptions() []grpc.DialOption {
	_ = initAndRegisterGRPCClientMetrics()
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
	}
}
