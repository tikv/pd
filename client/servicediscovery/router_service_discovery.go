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

package servicediscovery

import (
	"context"
	"crypto/tls"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/log"

	"github.com/tikv/pd/client/clients/metastorage"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/retry"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
	"github.com/tikv/pd/client/pkg/utils/tlsutil"
)

const (
	// "/ms/{cluster_id}/router/registry/"
	servicePathFormat = "/ms/%d/router/registry/"
)

var _ ServiceDiscovery = (*routerServiceDiscovery)(nil)

// routerServiceDiscovery implements ServiceDiscovery for the router.
type routerServiceDiscovery struct {
	ServiceDiscovery

	metaCli metastorage.Client
	// defaultDiscoveryKey is the etcd path used for discovering the serving endpoints of
	// the default keyspace group
	defaultDiscoveryKey string

	sortedUrls atomic.Value // Store as []string
	balancer   *serviceBalancer
	nodes      sync.Map // Store as map[string]serviceClient

	// URL -> a gRPC connection
	clientConns sync.Map // Store as map[string]*grpc.ClientConn

	callbacks *serviceCallbacks

	checkMembershipCh chan struct{}

	parentCtx context.Context
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	// Client option.
	option *opt.Option
	tlsCfg *tls.Config
}

// GetServiceURLs returns the URLs of the router service.
func (r *routerServiceDiscovery) GetServiceURLs() []string {
	if r.sortedUrls.Load() == nil {
		return nil
	}
	return r.sortedUrls.Load().([]string)
}

// GetClientConns returns the gRPC client connections to the router service.
func (r *routerServiceDiscovery) GetClientConns() *sync.Map {
	return &r.clientConns
}

// GetServiceClient returns the ServiceClient of the router service.
func (r *routerServiceDiscovery) GetServiceClient() ServiceClient {
	return r.balancer.get()
}

// GetServiceClientByKind returns the ServiceClient of the router service.
func (r *routerServiceDiscovery) GetServiceClientByKind(_ APIKind) ServiceClient {
	return r.GetServiceClient()
}

// GetOrCreateGRPCConn creates a gRPC connection to the router service.
func (r *routerServiceDiscovery) GetOrCreateGRPCConn(url string) (*grpc.ClientConn, error) {
	if r.ctx == nil {
		return nil, errs.ErrClientRouterServiceNotInitialized.FastGen("router service discovery is not initialized")
	}
	return grpcutil.GetOrCreateGRPCConn(r.ctx, &r.clientConns, url, r.tlsCfg, r.option.GRPCDialOptions...)
}

// ScheduleCheckMemberChanged schedules a check for member changes.
func (r *routerServiceDiscovery) ScheduleCheckMemberChanged() {
	select {
	case <-r.parentCtx.Done():
		log.Info("[router service] service discovery is shutting down")
	case r.checkMembershipCh <- struct{}{}:
	default:
	}
}

// CheckMemberChanged checks if there is any membership change among the members of the router service.
func (r *routerServiceDiscovery) CheckMemberChanged() error {
	if err := retry.WithConfig(r.ctx, r.updateMember); err != nil {
		log.Warn("[router service] failed to update member", errs.ZapError(err))
		return err
	}
	return nil
}

// AddMembersChangedCallback adds a callback to the router service discovery.
func (r *routerServiceDiscovery) AddMembersChangedCallback(cb func()) {
	r.callbacks.addMembersChangedCallback(cb)
}

// NewRouterServiceDiscovery returns a new client-side service discovery for the independent router service.
func NewRouterServiceDiscovery(
	ctx context.Context, metaCli metastorage.Client, serviceDiscovery ServiceDiscovery,
	tlsCfg *tls.Config, option *opt.Option,
) ServiceDiscovery {
	balancer := newServiceBalancer(emptyErrorFn)
	c := &routerServiceDiscovery{
		parentCtx:         ctx,
		ServiceDiscovery:  serviceDiscovery,
		metaCli:           metaCli,
		tlsCfg:            tlsCfg,
		option:            option,
		checkMembershipCh: make(chan struct{}, 1),
		balancer:          balancer,
		callbacks:         newServiceCallbacks(),
	}
	// Start with the default keyspace group. The actual keyspace group, to which the keyspace belongs,
	// will be discovered later.
	c.defaultDiscoveryKey = fmt.Sprintf(servicePathFormat, c.GetClusterID())

	log.Info("[router service] created router service discovery",
		zap.Uint64("cluster-id", c.GetClusterID()),
		zap.Uint32("keyspace-id", c.GetKeyspaceID()),
		zap.String("default-discovery-key", c.defaultDiscoveryKey))
	return c
}

// Init initialize the concrete client underlying
func (r *routerServiceDiscovery) Init() error {
	log.Info("[router service] initializing router service discovery",
		zap.Int("max-retry-times", r.option.MaxRetryTimes),
		zap.Duration("retry-interval", initRetryInterval))
	r.ctx, r.cancel = context.WithCancel(r.parentCtx)
	if err := r.CheckMemberChanged(); err != nil {
		// Initial check failed, log and continue to run the background loop.
		log.Warn("[router service] failed to initialize router service discovery", zap.Error(err))
	}
	r.wg.Add(2)
	go r.startCheckMemberLoop()
	go r.nodeHealthCheckLoop()
	return nil
}

func (r *routerServiceDiscovery) updateMember() error {
	urls, err := metastorage.DiscoveryMSAddrs(r.ctx, r.defaultDiscoveryKey, r.metaCli)
	if err != nil {
		// if met the meta storage client error, stop the router service discovery not retry again.
		if errs.ErrClientGetMetaStorageClient.Equal(err) || errs.ErrClientGetProtoClient.Equal(err) {
			log.Warn("[router service] meta storage client error, stopping router service discovery", errs.ZapError(err))
			return nil
		}
		return err
	}
	changed := r.nodesChanged(urls)
	if !changed {
		return nil
	}
	r.updateURLs(urls)
	r.updateNodes(urls)
	return nil
}

func (r *routerServiceDiscovery) updateNodes(urls []string) {
	urlSet := make(map[string]struct{}, len(urls))
	for _, url := range urls {
		if len(url) == 0 {
			continue
		}

		newURL := tlsutil.PickMatchedURL([]string{url}, r.tlsCfg)
		urlSet[newURL] = struct{}{}
		// If the node and client already exists, skip it.
		if client, ok := r.nodes.Load(newURL); !ok || client.(*serviceClient).GetClientConn() == nil {
			conn, err := r.GetOrCreateGRPCConn(newURL)
			if err != nil || conn == nil {
				log.Warn("[router service] failed to connect router service",
					zap.String("url", newURL), errs.ZapError(err))
				continue
			}
			nodeClient := newPDServiceClient(newURL, r.GetServingURL(), conn, false)
			r.nodes.LoadOrStore(newURL, nodeClient)
		}
	}
	clients := make([]ServiceClient, 0, len(urls))
	r.nodes.Range(func(key, value any) bool {
		url := key.(string)
		if _, exists := urlSet[url]; !exists {
			r.nodes.Delete(url)
			if cc, ok := r.clientConns.LoadAndDelete(url); ok {
				if err := cc.(*grpc.ClientConn).Close(); err != nil {
					log.Warn("[router service] failed to close stale gRPC connection",
						zap.String("url", url), errs.ZapError(err))
				}
			}
			log.Info("[router service] removed stale node", zap.String("url", url))
			return true
		}
		clients = append(clients, value.(*serviceClient))
		return true
	})
	log.Info("[router service] updating nodes succeeded",
		zap.Strings("urls", urls),
		zap.Int("clients-length", len(clients)))
	r.balancer.set(clients)
}

func (r *routerServiceDiscovery) nodesChanged(urls []string) bool {
	sort.Strings(urls)
	oldURLs := r.GetServiceURLs()
	return !reflect.DeepEqual(oldURLs, urls)
}

func (r *routerServiceDiscovery) updateURLs(urls []string) {
	sort.Strings(urls)
	r.sortedUrls.Store(urls)
	// Run callbacks to reflect the membership changes in the leader and followers.
	r.callbacks.onMembersChanged()
	log.Info("[router service] update member sorted urls", zap.Strings("new-sorted-urls", urls))
}

func (r *routerServiceDiscovery) startCheckMemberLoop() {
	defer r.wg.Done()

	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()
	ticker := time.NewTicker(MemberUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.checkMembershipCh:
		case <-ticker.C:
		case <-ctx.Done():
			log.Info("[router service] exit check member loop")
			return
		}
		// Make sure queryRetryMaxTimes * queryRetryInterval is far less than memberUpdateInterval,
		// so that we can speed up the process of router service discovery when failover happens on the
		// router service side and also ensures it won't call updateMember too frequently during normal time.
		if err := r.CheckMemberChanged(); err != nil {
			log.Warn("[router service] failed to update member", errs.ZapError(err))
		}
	}
}

// Close releases all resources
func (r *routerServiceDiscovery) Close() {
	log.Info("[router service] closing router service discovery")
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()

	r.clientConns.Range(func(key, cc any) bool {
		if err := cc.(*grpc.ClientConn).Close(); err != nil {
			log.Warn("[router service] failed to close gRPC clientConn", errs.ZapError(errs.ErrCloseGRPCConn, err))
		}
		r.clientConns.Delete(key)
		return true
	})
	r.sortedUrls.Store([]string{})
	r.balancer.clean()
	r.nodes.Clear()
	log.Info("[router service] is closed")
}

func (r *routerServiceDiscovery) nodeHealthCheckLoop() {
	defer r.wg.Done()

	nodeCheckLoopCtx, nodeCheckLoopCancel := context.WithCancel(r.ctx)
	defer nodeCheckLoopCancel()

	ticker := time.NewTicker(MemberHealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			log.Info("[router service] exit health check member loop")
			return
		case <-ticker.C:
			r.checkNodeHealth(nodeCheckLoopCtx)
		}
	}
}

func (r *routerServiceDiscovery) checkNodeHealth(ctx context.Context) {
	r.nodes.Range(func(_, value any) bool {
		// To ensure that the leader's healthy check is not delayed, shorten the duration.
		ctx, cancel := context.WithTimeout(ctx, MemberHealthCheckInterval/3)
		defer cancel()
		serviceClient := value.(*serviceClient)
		serviceClient.checkNetworkAvailable(ctx)
		return true
	})
	r.balancer.check()
}
