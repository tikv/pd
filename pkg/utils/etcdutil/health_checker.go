// Copyright 2024 TiKV Project Authors.
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

package etcdutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// healthyClient will wrap an etcd client and record its last health time.
// The etcd client inside will only maintain one connection to the etcd server
// to make sure each healthyClient could be used to check the health of a certain
// etcd endpoint without involving the load balancer of etcd client.
type healthyClient struct {
	*clientv3.Client
	lastHealth time.Time
}

// healthChecker is used to check the health of etcd endpoints. Inside the checker,
// we will maintain a map from each available etcd endpoint to its healthyClient.
type healthChecker struct {
	tickerInterval time.Duration
	tlsConfig      *tls.Config

	// Store as endpoint(string) -> *healthyClient
	healthyClients sync.Map
	// client is the etcd client the health checker is guarding, it will be set with
	// the checked healthy endpoints dynamically and periodically.
	client *clientv3.Client
}

// initHealthChecker initializes the health checker for etcd client.
func initHealthChecker(tickerInterval time.Duration, tlsConfig *tls.Config, client *clientv3.Client) {
	healthChecker := &healthChecker{
		tickerInterval: tickerInterval,
		tlsConfig:      tlsConfig,
		client:         client,
	}
	// A health checker has the same lifetime with the given etcd client.
	ctx := client.Ctx()
	// Sync etcd endpoints and check the last health time of each endpoint periodically.
	go healthChecker.syncer(ctx)
	// Inspect the health of each endpoint by reading the health key periodically.
	go healthChecker.inspector(ctx)
}

func (checker *healthChecker) syncer(ctx context.Context) {
	defer logutil.LogPanic()
	checker.update()
	ticker := time.NewTicker(checker.tickerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("etcd client is closed, exit update endpoint goroutine")
			return
		case <-ticker.C:
			checker.update()
		}
	}
}

func (checker *healthChecker) inspector(ctx context.Context) {
	defer logutil.LogPanic()
	ticker := time.NewTicker(checker.tickerInterval)
	defer ticker.Stop()
	lastAvailable := time.Now()
	for {
		select {
		case <-ctx.Done():
			log.Info("etcd client is closed, exit health check goroutine")
			checker.close()
			return
		case <-ticker.C:
			lastEps := checker.client.Endpoints()
			healthyEps := checker.patrol(ctx)
			if len(healthyEps) == 0 {
				// when no endpoint could be used, try to reset endpoints to update connect rather
				// than delete them to avoid there is no any endpoint in client.
				// Note: reset endpoints will trigger sub-connection closed, and then trigger reconnection.
				// Otherwise, the sub-connection will be retrying in gRPC layer and use exponential backoff,
				// and it cannot recover as soon as possible.
				if time.Since(lastAvailable) > etcdServerDisconnectedTimeout {
					log.Info("no available endpoint, try to reset endpoints",
						zap.Strings("last-endpoints", lastEps))
					resetClientEndpoints(checker.client, lastEps...)
				}
			} else {
				if !typeutil.AreStringSlicesEquivalent(healthyEps, lastEps) {
					oldNum, newNum := len(lastEps), len(healthyEps)
					checker.client.SetEndpoints(healthyEps...)
					etcdStateGauge.WithLabelValues("endpoints").Set(float64(newNum))
					log.Info("update endpoints",
						zap.String("num-change", fmt.Sprintf("%d->%d", oldNum, newNum)),
						zap.Strings("last-endpoints", lastEps),
						zap.Strings("endpoints", checker.client.Endpoints()))
				}
				lastAvailable = time.Now()
			}
		}
	}
}

func (checker *healthChecker) close() {
	checker.healthyClients.Range(func(key, value interface{}) bool {
		client := value.(*healthyClient)
		client.Close()
		return true
	})
}

// Reset the etcd client endpoints to trigger reconnect.
func resetClientEndpoints(client *clientv3.Client, endpoints ...string) {
	client.SetEndpoints()
	client.SetEndpoints(endpoints...)
}

// See https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/etcdctl/ctlv3/command/ep_command.go#L105-L145
func (checker *healthChecker) patrol(ctx context.Context) []string {
	var (
		count       = checker.clientCount()
		hch         = make(chan string, count)
		healthyList = make([]string, 0, count)
		wg          sync.WaitGroup
	)
	checker.healthyClients.Range(func(key, value interface{}) bool {
		wg.Add(1)
		go func(key, value interface{}) {
			defer wg.Done()
			defer logutil.LogPanic()
			var (
				ep     = key.(string)
				client = value.(*healthyClient)
			)
			if IsHealthy(ctx, client.Client) {
				hch <- ep
				checker.storeClient(ep, &healthyClient{
					Client:     client.Client,
					lastHealth: time.Now(),
				})
				return
			}
		}(key, value)
		return true
	})
	wg.Wait()
	close(hch)
	for h := range hch {
		healthyList = append(healthyList, h)
	}
	return healthyList
}

func (checker *healthChecker) update() {
	eps := syncUrls(checker.client)
	if len(eps) == 0 {
		log.Warn("no available etcd endpoint returned by etcd cluster")
		return
	}
	epMap := make(map[string]struct{}, len(eps))
	for _, ep := range eps {
		epMap[ep] = struct{}{}
	}
	// Check if client exists:
	//   - If not, create one.
	//   - If exists, check if it's offline or disconnected for a long time.
	for ep := range epMap {
		client := checker.loadClient(ep)
		if client == nil {
			checker.addClient(ep, time.Now())
			continue
		}
		since := time.Since(client.lastHealth)
		// Check if it's offline for a long time and try to remove it.
		if since > etcdServerOfflineTimeout {
			log.Info("etcd server might be offline, try to remove it",
				zap.Duration("since-last-health", since),
				zap.String("endpoint", ep))
			checker.removeClient(ep)
			continue
		}
		// Check if it's disconnected for a long time and try to reconnect.
		if since > etcdServerDisconnectedTimeout {
			log.Info("etcd server might be disconnected, try to reconnect",
				zap.Duration("since-last-health", since),
				zap.String("endpoint", ep))
			resetClientEndpoints(client.Client, ep)
		}
	}
	// Clean up the stale clients which are not in the etcd cluster anymore.
	checker.healthyClients.Range(func(key, value interface{}) bool {
		ep := key.(string)
		if _, ok := epMap[ep]; !ok {
			log.Info("remove stale etcd client", zap.String("endpoint", ep))
			checker.removeClient(ep)
		}
		return true
	})
}

func (checker *healthChecker) clientCount() int {
	count := 0
	checker.healthyClients.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

func (checker *healthChecker) loadClient(ep string) *healthyClient {
	if client, ok := checker.healthyClients.Load(ep); ok {
		return client.(*healthyClient)
	}
	return nil
}

func (checker *healthChecker) addClient(ep string, lastHealth time.Time) {
	client, err := newClient(checker.tlsConfig, ep)
	if err != nil {
		log.Error("failed to create etcd healthy client",
			zap.String("endpoint", ep),
			zap.Error(err))
		return
	}
	checker.healthyClients.Store(ep, &healthyClient{
		Client:     client,
		lastHealth: lastHealth,
	})
}

func (checker *healthChecker) storeClient(ep string, client *healthyClient) {
	checker.healthyClients.Store(ep, client)
}

func (checker *healthChecker) removeClient(ep string) {
	if client, ok := checker.healthyClients.LoadAndDelete(ep); ok {
		err := client.(*healthyClient).Close()
		if err != nil {
			log.Error("failed to close etcd healthy client",
				zap.String("endpoint", ep),
				zap.Error(err))
		}
	}
}

// See https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/clientv3/client.go#L170-L183
func syncUrls(client *clientv3.Client) (eps []string) {
	resp, err := ListEtcdMembers(clientv3.WithRequireLeader(client.Ctx()), client)
	if err != nil {
		log.Error("failed to list members", errs.ZapError(err))
		return nil
	}
	for _, m := range resp.Members {
		if len(m.Name) == 0 || m.IsLearner {
			continue
		}
		eps = append(eps, m.ClientURLs...)
	}
	return eps
}
