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

package server

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"

	"github.com/tikv/pd/pkg/utils/grpcutil"
)

const (
	namespace       = "meta_storage"
	serverSubsystem = "server"
)

var grpcStreamSendDuration = grpcutil.NewGRPCStreamSendDuration(namespace, serverSubsystem)

func init() {
	prometheus.MustRegister(grpcStreamSendDuration)
}

func newWatchMetricsStream(server meta_storagepb.MetaStorage_WatchServer) meta_storagepb.MetaStorage_WatchServer {
	return &grpcutil.MetricsStream[*meta_storagepb.WatchResponse, any]{
		ServerStream: server,
		SendFn:       server.Send,
		SendObs:      grpcStreamSendDuration.WithLabelValues("watch"),
	}
}
