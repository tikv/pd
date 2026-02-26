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

package server

import (
	"io"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"

	"github.com/tikv/pd/pkg/utils/grpcutil"
)

const (
	namespace       = "meta_storage"
	serverSubsystem = "server"
)

var (
	grpcStreamOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "grpc_stream_operation_duration_seconds",
			Help:      "Bucketed histogram of duration (s) of gRPC stream Send/Recv operations.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms ~ 52s
		}, []string{"request", "type"})
)

func init() {
	prometheus.MustRegister(grpcStreamOperationDuration)
}

func newWatchMetricsStream(server meta_storagepb.MetaStorage_WatchServer) meta_storagepb.MetaStorage_WatchServer {
	return &grpcutil.MetricsStream[*meta_storagepb.WatchResponse, any]{
		ServerStream: server,
		SendFn:       server.Send,
		RecvFn:       func() (any, error) { return nil, io.EOF },
		SendObs:      grpcStreamOperationDuration.WithLabelValues("watch", "send"),
	}
}
