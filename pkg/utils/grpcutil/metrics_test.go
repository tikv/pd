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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

func TestQueryRegionRequestMetrics(t *testing.T) {
	for _, mode := range []string{"mixed", "header", "malformed", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			re := require.New(t)
			counter := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "region_request_cnt"},
				[]string{"request", "caller_id", "caller_component", "event"})
			request := &pdpb.QueryRegionRequest{
				Header:   &pdpb.RequestHeader{CallerId: "client", CallerComponent: "fallback"},
				Keys:     [][]byte{[]byte("same"), []byte("same"), []byte("same")},
				PrevKeys: [][]byte{[]byte("same"), []byte("same")},
				Ids:      []uint64{42, 42},
			}
			type observation struct {
				method, component string
				count             float64
			}
			expected := []observation{{"GetRegion", "fallback", 3}, {"GetPrevRegion", "fallback", 2}, {"GetRegionByID", "fallback", 2}}
			callerID := "client"
			switch mode {
			case "mixed":
				request.KeyCallerComponents = []string{"a", "b", "a"}
				request.PrevKeyCallerComponents = []string{"b", ""}
				request.IdCallerComponents = []string{"c", "a"}
				expected = []observation{{"GetRegion", "a", 2}, {"GetRegion", "b", 1}, {"GetPrevRegion", "b", 1}, {"GetPrevRegion", "unknown", 1}, {"GetRegionByID", "c", 1}, {"GetRegionByID", "a", 1}}
			case "malformed":
				request.KeyCallerComponents = []string{"wrong"}
				request.PrevKeyCallerComponents = []string{"wrong", "wrong", "wrong"}
				request.IdCallerComponents = []string{"wrong"}
			case "unknown":
				request.Header = nil
				callerID = "unknown"
				for i := range expected {
					expected[i].component = "unknown"
				}
			}
			// Failures are not sampled, so attribution and counts are exact.
			RecordQueryRegionRequestMetrics(request, &pdpb.Error{Type: pdpb.ErrorType_NOT_BOOTSTRAPPED}, counter)
			re.Equal(len(expected), testutil.CollectAndCount(counter))
			for _, want := range expected {
				re.Equal(want.count, testutil.ToFloat64(counter.WithLabelValues(want.method, callerID, want.component, "failed")))
			}
			// Empty messages contain no logical requests and create no series.
			counter.Reset()
			RecordQueryRegionRequestMetrics(&pdpb.QueryRegionRequest{}, nil, counter)
			re.Zero(testutil.CollectAndCount(counter))
		})
	}
}
