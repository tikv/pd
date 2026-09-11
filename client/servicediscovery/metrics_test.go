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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	clientmetrics "github.com/tikv/pd/client/metrics"
)

func TestServiceDiscoveryMetricsInitializationIsConcurrentSafe(t *testing.T) {
	const producerCount = 32
	var (
		ready sync.WaitGroup
		wg    sync.WaitGroup
		stop  atomic.Bool
	)
	t.Cleanup(func() {
		stop.Store(true)
		wg.Wait()
	})
	ready.Add(producerCount)
	wg.Add(producerCount)
	for range producerCount {
		go func() {
			defer wg.Done()
			loadServiceDiscoveryMetrics().getMembers.Observe(0)
			ready.Done()
			for !stop.Load() {
				loadServiceDiscoveryMetrics().getClusterInfo.Observe(0)
				loadServiceDiscoveryMetrics().getMembers.Observe(0)
			}
		}()
	}

	ready.Wait()
	clientmetrics.InitAndRegisterMetrics(prometheus.Labels{"instance": "test"})
	stop.Store(true)
	wg.Wait()

	loadServiceDiscoveryMetrics().getMembers.Observe(0)
}
