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
	"github.com/tikv/pd/client/constants"
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
	metacli          metastorage.Client
	serviceDiscovery ServiceDiscovery
	clusterID        uint64
	keyspaceID       atomic.Uint32
	// defaultDiscoveryKey is the etcd path used for discovering the serving endpoints of
	// the default keyspace group
	defaultDiscoveryKey string

	urls     atomic.Value // Store as []string
	balancer *serviceBalancer
	nodes    sync.Map // Store as map[string]serviceClient

	// URL -> a gRPC connection
	clientConns sync.Map // Store as map[string]*grpc.ClientConn

	callbacks *serviceCallbacks

	checkMembershipCh chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	tlsCfg *tls.Config

	// Client option.
	option *opt.Option
}

func (c *routerServiceDiscovery) GetClusterID() uint64 {
	return c.clusterID
}

func (c *routerServiceDiscovery) GetKeyspaceID() uint32 {
	return c.keyspaceID.Load()
}

func (c *routerServiceDiscovery) SetKeyspaceID(id uint32) {
	c.keyspaceID.Store(id)
}

func (c *routerServiceDiscovery) GetKeyspaceGroupID() uint32 {
	return constants.DefaultKeyspaceGroupID
}

func (c *routerServiceDiscovery) GetServiceURLs() []string {
	if c.urls.Load() == nil {
		return nil
	}
	return c.urls.Load().([]string)
}

func (c *routerServiceDiscovery) GetServingEndpointClientConn() *grpc.ClientConn {
	log.Warn("[router-service] GetServingEndpointClientConn not supported")
	return nil
}

func (c *routerServiceDiscovery) GetClientConns() *sync.Map {
	return &c.clientConns
}

func (c *routerServiceDiscovery) GetServingURL() string {
	log.Warn("[router-service]  GetServingURL not supported")
	return ""
}

func (c *routerServiceDiscovery) GetBackupURLs() []string {
	return nil
}

func (c *routerServiceDiscovery) GetServiceClient() ServiceClient {
	return c.GetServiceClientByKind(ForwardAPIKind)
}

func (c *routerServiceDiscovery) GetServiceClientByKind(_ APIKind) ServiceClient {
	client := c.balancer.get()
	if client == nil {
		return nil
	}
	return client
}

func (c *routerServiceDiscovery) GetAllServiceClients() []ServiceClient {
	return c.serviceDiscovery.GetAllServiceClients()
}

func (c *routerServiceDiscovery) GetOrCreateGRPCConn(url string) (*grpc.ClientConn, error) {
	return grpcutil.GetOrCreateGRPCConn(c.ctx, &c.clientConns, url, c.tlsCfg, c.option.GRPCDialOptions...)
}

// ScheduleCheckMemberChanged schedules a check for member changes.
func (c *routerServiceDiscovery) ScheduleCheckMemberChanged() {
	select {
	case c.checkMembershipCh <- struct{}{}:
	default:
	}
}

func (c *routerServiceDiscovery) CheckMemberChanged() error {
	if err := c.serviceDiscovery.CheckMemberChanged(); err != nil {
		log.Warn("[router] failed to check member changed", errs.ZapError(err))
	}
	if err := innerRetry(c.ctx, queryRetryMaxTimes, c.updateMember); err != nil {
		log.Error("[router] failed to update member", errs.ZapError(err))
		return err
	}
	return nil
}

func (c *routerServiceDiscovery) ExecAndAddLeaderSwitchedCallback(_cb LeaderSwitchedCallbackFunc) {
	log.Warn("[router] leader switched callback function is disabled")
}

func (c *routerServiceDiscovery) AddLeaderSwitchedCallback(_cb LeaderSwitchedCallbackFunc) {
	log.Warn("[router] leader switched callback function is disabled")
}

func (c *routerServiceDiscovery) AddMembersChangedCallback(cb func()) {
	c.callbacks.addMembersChangedCallback(cb)
}

// NewRouterServiceDiscovery returns a new client-side service discovery for the independent router service.
func NewRouterServiceDiscovery(
	ctx context.Context, metacli metastorage.Client, serviceDiscovery ServiceDiscovery,
	keyspaceID uint32, tlsCfg *tls.Config, option *opt.Option,
) ServiceDiscovery {
	ctx, cancel := context.WithCancel(ctx)
	balancer := newServiceBalancer(emptyErrorFn)
	c := &routerServiceDiscovery{
		ctx:               ctx,
		cancel:            cancel,
		metacli:           metacli,
		serviceDiscovery:  serviceDiscovery,
		clusterID:         serviceDiscovery.GetClusterID(),
		tlsCfg:            tlsCfg,
		option:            option,
		checkMembershipCh: make(chan struct{}, 1),
		balancer:          balancer,
		callbacks:         newServiceCallbacks(),
	}
	c.keyspaceID.Store(keyspaceID)
	// Start with the default keyspace group. The actual keyspace group, to which the keyspace belongs,
	// will be discovered later.
	c.defaultDiscoveryKey = fmt.Sprintf(servicePathFormat, c.clusterID)

	log.Info("created router service discovery",
		zap.Uint64("cluster-id", c.clusterID),
		zap.Uint32("keyspace-id", keyspaceID),
		zap.String("default-discovery-key", c.defaultDiscoveryKey))

	if err := c.Init(); err != nil {
		log.Warn("[router] failed to init router service discovery")
	}
	return c
}

// Init initialize the concrete client underlying
func (c *routerServiceDiscovery) Init() error {
	log.Info("initializing router service discovery",
		zap.Int("max-retry-times", c.option.MaxRetryTimes),
		zap.Duration("retry-interval", initRetryInterval))
	if err := innerRetry(c.ctx, c.option.MaxRetryTimes, c.updateMember); err != nil {
		log.Error("failed to update member. initialization failed.", zap.Error(err))
		c.cancel()
		return err
	}
	c.wg.Add(2)
	go c.startCheckMemberLoop()
	go c.nodeHealthCheckLoop()
	return nil
}

func (c *routerServiceDiscovery) updateMember() error {
	urls, err := getMSMembers(c.ctx, c.defaultDiscoveryKey, c.metacli)
	if err != nil {
		return err
	}
	if len(urls) == 0 {
		return errs.ErrClientNoAvailableMember.GenWithStackByArgs()
	}
	changed := c.nodesChanged(urls)
	if !changed {
		return nil
	}
	c.updateURLs(urls)
	c.updateNodes(urls)
	return nil
}

func (c *routerServiceDiscovery) updateNodes(urls []string) {
	nodes := make(map[string]*serviceClient)
	c.nodes.Range(func(key, value any) bool {
		nodes[key.(string)] = value.(*serviceClient)
		return true
	})

	for _, url := range urls {
		if len(url) > 0 {
			newUrl := tlsutil.PickMatchedURL([]string{url}, c.tlsCfg)
			if client, ok := c.nodes.Load(newUrl); ok {
				if client.(*serviceClient).GetClientConn() == nil {
					conn, err := c.GetOrCreateGRPCConn(url)
					if err != nil || conn == nil {
						log.Warn("[pd] failed to connect follower", zap.String("follower", url), errs.ZapError(err))
						continue
					}
					node := newPDServiceClient(url, c.GetServingURL(), conn, false)
					c.nodes.Store(url, node)
				}
			} else {
				conn, err := c.GetOrCreateGRPCConn(url)
				follower := newPDServiceClient(url, c.GetServingURL(), conn, false)
				if err != nil || conn == nil {
					log.Warn("[pd] failed to connect follower", zap.String("follower", url), errs.ZapError(err))
				}
				c.nodes.LoadOrStore(url, follower)
			}
		}
	}
	clients := make([]ServiceClient, 0, len(urls))
	c.nodes.Range(func(_, value interface{}) bool {
		clients = append(clients, value.(*serviceClient))
		return true
	})
	c.balancer.set(clients)
	return
}

func (c *routerServiceDiscovery) nodesChanged(urls []string) bool {
	sort.Strings(urls)
	oldURLs := c.GetServiceURLs()
	return !reflect.DeepEqual(oldURLs, urls)
}

func (c *routerServiceDiscovery) updateURLs(urls []string) {
	sort.Strings(urls)
	c.urls.Store(urls)
	// Run callbacks to reflect the membership changes in the leader and followers.
	c.callbacks.onMembersChanged()
	log.Info("[router] update member urls", zap.Strings("new-urls", urls))
}

func (c *routerServiceDiscovery) startCheckMemberLoop() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	ticker := time.NewTicker(MemberUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.checkMembershipCh:
		case <-ticker.C:
		case <-ctx.Done():
			log.Info("[router] exit check member loop")
			return
		}
		// Make sure queryRetryMaxTimes * queryRetryInterval is far less than memberUpdateInterval,
		// so that we can speed up the process of router service discovery when failover happens on the
		// router service side and also ensures it won't call updateMember too frequently during normal time.
		if err := innerRetry(c.ctx, queryRetryMaxTimes, c.updateMember); err != nil {
			log.Error("[router] failed to update member", errs.ZapError(err))
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
func (c *routerServiceDiscovery) Close() {
	if c == nil {
		return
	}
	log.Info("closing router service discovery")
	c.cancel()
	c.wg.Wait()

	c.clientConns.Range(func(key, cc any) bool {
		if err := cc.(*grpc.ClientConn).Close(); err != nil {
			log.Error("[router service] failed to close gRPC clientConn", errs.ZapError(errs.ErrCloseGRPCConn, err))
		}
		c.clientConns.Delete(key)
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

func (c *routerServiceDiscovery) nodeHealthCheckLoop() {
	defer c.wg.Done()

	nodeCheckLoopCtx, nodeCheckLoopCancel := context.WithCancel(c.ctx)
	defer nodeCheckLoopCancel()

	ticker := time.NewTicker(MemberHealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.checkNodeHealth(nodeCheckLoopCtx)
		}
	}
}

func (c *routerServiceDiscovery) checkNodeHealth(ctx context.Context) {
	c.nodes.Range(func(_, value any) bool {
		// To ensure that the leader's healthy check is not delayed, shorten the duration.
		ctx, cancel := context.WithTimeout(ctx, MemberHealthCheckInterval/3)
		defer cancel()
		serviceClient := value.(*serviceClient)
		serviceClient.checkNetworkAvailable(ctx)
		return true
	})
	c.balancer.check()
}
