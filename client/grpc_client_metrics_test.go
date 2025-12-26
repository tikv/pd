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
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestGRPCClientMetricsRegisteredAndIdempotent(t *testing.T) {
	re := require.New(t)

	m1 := initAndRegisterGRPCClientMetrics()
	re.NotNil(m1)
	m2 := initAndRegisterGRPCClientMetrics()
	re.NotNil(m2)
	re.Same(m1, m2)

	// Touch the interceptor once so CounterVec/HistogramVec creates a time series and
	// shows up in Gather() output.
	interceptor := m1.UnaryClientInterceptor()
	err := interceptor(
		context.Background(),
		"/pd.test.Greeter/SayHello",
		struct{}{},
		&struct{}{},
		(*grpc.ClientConn)(nil),
		func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			return nil
		},
	)
	re.NoError(err)

	mfs, err := prometheus.DefaultGatherer.Gather()
	re.NoError(err)

	found := false
	for _, mf := range mfs {
		if mf == nil || mf.GetName() == "" {
			continue
		}
		switch mf.GetName() {
		case "grpc_client_started_total",
			"grpc_client_handled_total",
			"grpc_client_handling_seconds":
			found = true
		}
		if found {
			break
		}
	}
	re.True(found, "expected grpc_client_* metrics to be registered in default registry")
}
