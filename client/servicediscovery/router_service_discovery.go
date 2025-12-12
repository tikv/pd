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
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/clients/metastorage"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
	"github.com/tikv/pd/client/pkg/utils/tlsutil"
)

const (
	servicePathFormat = "/ms/%d/router/registry/" // "/ms/{cluster_id}/router/registry/"
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

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

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
	return r.GetServiceClientByKind(ForwardAPIKind)
}

// GetServiceClientByKind returns the ServiceClient of the router service.
func (r *routerServiceDiscovery) GetServiceClientByKind(_ APIKind) ServiceClient {
	client := r.balancer.get()
	if client == nil {
		return nil
	}
	return client
}

// GetOrCreateGRPCConn creates a gRPC connection to the router service.
func (r *routerServiceDiscovery) GetOrCreateGRPCConn(url string) (*grpc.ClientConn, error) {
	return grpcutil.GetOrCreateGRPCConn(r.ctx, &r.clientConns, url, r.tlsCfg, r.option.GRPCDialOptions...)
}

// ScheduleCheckMemberChanged schedules a check for member changes.
func (r *routerServiceDiscovery) ScheduleCheckMemberChanged() {
	select {
	case r.checkMembershipCh <- struct{}{}:
	default:
	}
}

// CheckMemberChanged checks if there is any membership change among the members of the router service.
func (r *routerServiceDiscovery) CheckMemberChanged() error {
	if err := innerRetry(r.ctx, queryRetryMaxTimes, r.updateMember); err != nil {
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
	ctx, cancel := context.WithCancel(ctx)
	balancer := newServiceBalancer(emptyErrorFn)
	c := &routerServiceDiscovery{
		ctx:               ctx,
		ServiceDiscovery:  serviceDiscovery,
		cancel:            cancel,
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

	log.Info("created router service discovery",
		zap.Uint64("cluster-id", c.GetClusterID()),
		zap.Uint32("keyspace-id", c.GetKeyspaceID()),
		zap.String("default-discovery-key", c.defaultDiscoveryKey))
	return c
}

// Init initialize the concrete client underlying
func (r *routerServiceDiscovery) Init() error {
	log.Info("initializing router service discovery",
		zap.Int("max-retry-times", r.option.MaxRetryTimes),
		zap.Duration("retry-interval", initRetryInterval))
	if err := r.CheckMemberChanged(); err != nil {
		r.cancel()
		log.Warn("failed to initialize router service discovery", zap.Error(err))
		return err
	}
	r.wg.Add(2)
	go r.startCheckMemberLoop()
	go r.nodeHealthCheckLoop()
	return nil
}

func (r *routerServiceDiscovery) updateMember() error {
	urls, err := getMSMembers(r.ctx, r.defaultDiscoveryKey, r.metaCli)
	if err != nil {
		return err
	}
	if len(urls) == 0 {
		return errs.ErrClientNoAvailableMember.GenWithStackByArgs()
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
	nodes := make(map[string]*serviceClient)
	r.nodes.Range(func(key, value any) bool {
		nodes[key.(string)] = value.(*serviceClient)
		return true
	})

	for _, url := range urls {
		if len(url) > 0 {
			newURL := tlsutil.PickMatchedURL([]string{url}, r.tlsCfg)
			if client, ok := r.nodes.Load(newURL); ok {
				if client.(*serviceClient).GetClientConn() == nil {
					conn, err := r.GetOrCreateGRPCConn(url)
					if err != nil || conn == nil {
						log.Warn("[pd] failed to connect follower", zap.String("follower", url), errs.ZapError(err))
						continue
					}
					node := newPDServiceClient(url, r.GetServingURL(), conn, false)
					r.nodes.Store(url, node)
				}
			} else {
				conn, err := r.GetOrCreateGRPCConn(url)
				follower := newPDServiceClient(url, r.GetServingURL(), conn, false)
				if err != nil || conn == nil {
					log.Warn("[pd] failed to connect follower", zap.String("follower", url), errs.ZapError(err))
				}
				r.nodes.LoadOrStore(url, follower)
			}
		}
	}
	clients := make([]ServiceClient, 0, len(urls))
	r.nodes.Range(func(_, value any) bool {
		clients = append(clients, value.(*serviceClient))
		return true
	})
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
	log.Info("[router service] update member sortedUrls", zap.Strings("new-sortedUrls", urls))
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
			log.Error("[router service] failed to update member", errs.ZapError(err))
		}
	}
}

func innerRetry(
	ctx context.Context, maxRetryTimes int, f func() error,
) error {
	var err error
	ticker := time.NewTicker(queryRetryInterval)
	defer ticker.Stop()
	for range maxRetryTimes {
		if err = f(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-ticker.C:
		}
	}
	return errors.WithStack(err)
}

// Close releases all resources
func (r *routerServiceDiscovery) Close() {
	log.Info("closing router service discovery")
	r.cancel()
	r.wg.Wait()

	r.clientConns.Range(func(key, cc any) bool {
		if err := cc.(*grpc.ClientConn).Close(); err != nil {
			log.Error("[router service] failed to close gRPC clientConn", errs.ZapError(errs.ErrCloseGRPCConn, err))
		}
		r.clientConns.Delete(key)
		return true
	})

	log.Info("router service discovery is closed")
}

// getMSMembers returns all the members of the specified service name.
func getMSMembers(ctx context.Context, serviceKey string, client metastorage.Client) ([]string, error) {
	resp, err := client.Get(ctx, []byte(serviceKey), opt.WithPrefix())
	if err != nil {
		return nil, errs.ErrClientGetMetaStorageClient.Wrap(err).GenWithStackByCause()
	}
	if err := resp.GetHeader().GetError(); err != nil {
		return nil, errs.ErrClientGetProtoClient.Wrap(errors.New(err.GetMessage())).GenWithStackByCause()
	}
	ret := make([]string, 0, len(resp.GetKvs()))
	for _, kv := range resp.GetKvs() {
		var entry ServiceRegistryEntry
		if err = entry.Deserialize(kv.Value); err != nil {
			log.Error("try to deserialize service registry entry failed", zap.String("key", string(kv.Key)), zap.Error(err))
			continue
		}
		ret = append(ret, entry.ServiceAddr)
	}
	return ret, nil
}

// ServiceRegistryEntry is the registry entry of a service
type ServiceRegistryEntry struct {
	// The specific value will be assigned only if the startup parameter is added.
	// If not assigned, the default value(service-hostname) will be used.
	Name           string `json:"name"`
	ServiceAddr    string `json:"service-addr"`
	Version        string `json:"version"`
	GitHash        string `json:"git-hash"`
	DeployPath     string `json:"deploy-path"`
	StartTimestamp int64  `json:"start-timestamp"`
}

// Serialize this service registry entry
func (e *ServiceRegistryEntry) Serialize() (serializedValue string, err error) {
	data, err := json.Marshal(e)
	if err != nil {
		log.Error("json marshal the service registry entry failed", zap.Error(err))
		return "", err
	}
	return string(data), nil
}

// Deserialize the data to this service registry entry
func (e *ServiceRegistryEntry) Deserialize(data []byte) error {
	if err := json.Unmarshal(data, e); err != nil {
		log.Error("json unmarshal the service registry entry failed", zap.Error(err))
		return err
	}
	return nil
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
