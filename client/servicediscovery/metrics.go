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

package servicediscovery

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	clientmetrics "github.com/tikv/pd/client/metrics"
)

type serviceDiscoveryMetrics struct {
	getClusterInfo       prometheus.Observer
	getClusterInfoFailed prometheus.Observer
	getMembers           prometheus.Observer
	getMembersFailed     prometheus.Observer
}

var currentServiceDiscoveryMetrics atomic.Pointer[serviceDiscoveryMetrics]

func init() {
	// An HTTP client can start service discovery before the first RPC client
	// rebuilds and registers client metrics. Publish the four handles together
	// so an active discovery loop never reads globals while they are replaced.
	clientmetrics.RegisterConsumer(func() {
		currentServiceDiscoveryMetrics.Store(&serviceDiscoveryMetrics{
			getClusterInfo:       clientmetrics.InternalCmdDurationGetClusterInfo,
			getClusterInfoFailed: clientmetrics.InternalCmdFailedDurationGetClusterInfo,
			getMembers:           clientmetrics.InternalCmdDurationGetMembers,
			getMembersFailed:     clientmetrics.InternalCmdFailedDurationGetMembers,
		})
	})
}

func loadServiceDiscoveryMetrics() *serviceDiscoveryMetrics {
	return currentServiceDiscoveryMetrics.Load()
}
