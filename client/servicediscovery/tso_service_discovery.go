// Copyright 2023 TiKV Project Authors.
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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/clients/metastorage"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
)

const (
	// tsoSvcDiscoveryFormat defines the key prefix for keyspace group primary election.
	// The entire key is in the format of "/ms/<cluster-id>/tso/<group-id>/primary".
	// The <group-id> is 5 digits integer with leading zeros.
	tsoSvcDiscoveryFormat = "/ms/%d/tso/%05d/primary"
	// initRetryInterval is the rpc retry interval during the initialization phase.
	initRetryInterval = time.Second
	// tsoQueryRetryMaxTimes is the max retry times for querying TSO.
	tsoQueryRetryMaxTimes = 10
	// tsoQueryRetryInterval is the retry interval for querying TSO.
	tsoQueryRetryInterval = 500 * time.Millisecond
)

var _ ServiceDiscovery = (*tsoServiceDiscovery)(nil)

// keyspaceGroupSvcDiscovery is used for discovering the serving endpoints of the keyspace
// group to which the keyspace belongs
type keyspaceGroupSvcDiscovery struct {
	sync.RWMutex
	group *tsopb.KeyspaceGroup
	// primaryURL is the primary serving URL
	primaryURL string
	// secondaryURLs are TSO secondary serving URL
	secondaryURLs []string
	// urls are the primary/secondary serving URL
	urls []string
}

func (k *keyspaceGroupSvcDiscovery) update(
	keyspaceGroup *tsopb.KeyspaceGroup,
	newPrimaryURL string,
	secondaryURLs, urls []string,
) (oldPrimaryURL string, primarySwitched, secondaryChanged bool) {
	k.Lock()
	defer k.Unlock()

	// If the new primary URL is empty, we don't switch the primary URL.
	oldPrimaryURL = k.primaryURL
	if len(newPrimaryURL) > 0 {
		primarySwitched = !strings.EqualFold(oldPrimaryURL, newPrimaryURL)
		k.primaryURL = newPrimaryURL
	}

	if !reflect.DeepEqual(k.secondaryURLs, secondaryURLs) {
		k.secondaryURLs = secondaryURLs
		secondaryChanged = true
	}

	k.group = keyspaceGroup
	k.urls = urls
	return
}

// tsoServerDiscovery is for discovering the serving endpoints of the TSO servers
// TODO: dynamically update the TSO server URLs in the case of TSO server failover
// and scale-out/in.
type tsoServerDiscovery struct {
	sync.RWMutex
	urls []string
	// used for round-robin load balancing
	selectIdx int
}

// tsoServiceDiscovery is the service discovery client of the independent TSO service

type tsoServiceDiscovery struct {
	metacli          metastorage.Client
	serviceDiscovery ServiceDiscovery
	clusterID        uint64
	keyspaceID       atomic.Uint32
	keyspaceMeta     *keyspacepb.KeyspaceMeta // keyspace metadata

	// defaultDiscoveryKey is the etcd path used for discovering the serving endpoints of
	// the default keyspace group
	defaultDiscoveryKey string
	// tsoServersDiscovery is for discovering the serving endpoints of the TSO servers
	*tsoServerDiscovery

	// keyspaceGroupSD is for discovering the serving endpoints of the keyspace group
	keyspaceGroupSD *keyspaceGroupSvcDiscovery

	// URL -> a gRPC connection
	clientConns sync.Map // Store as map[string]*grpc.ClientConn

	// tsoLeaderUpdatedCb will be called when the TSO leader is updated.
	tsoLeaderUpdatedCb LeaderSwitchedCallbackFunc

	checkMembershipCh chan struct{}

	ctx                  context.Context
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
	printFallbackLogOnce sync.Once

	tlsCfg *tls.Config

	// Client option.
	option *opt.Option
}

// NewTSOServiceDiscovery returns a new client-side service discovery for the independent TSO service.
func NewTSOServiceDiscovery(
	ctx context.Context, metacli metastorage.Client, serviceDiscovery ServiceDiscovery,
	keyspaceID uint32, keyspaceMeta *keyspacepb.KeyspaceMeta, tlsCfg *tls.Config, option *opt.Option,
) ServiceDiscovery {
	ctx, cancel := context.WithCancel(ctx)
	c := &tsoServiceDiscovery{
		ctx:               ctx,
		cancel:            cancel,
		metacli:           metacli,
		serviceDiscovery:  serviceDiscovery,
		clusterID:         serviceDiscovery.GetClusterID(),
		keyspaceMeta:      keyspaceMeta,
		tlsCfg:            tlsCfg,
		option:            option,
		checkMembershipCh: make(chan struct{}, 1),
	}
	c.keyspaceID.Store(keyspaceID)
	c.keyspaceGroupSD = &keyspaceGroupSvcDiscovery{
		primaryURL:    "",
		secondaryURLs: make([]string, 0),
		urls:          make([]string, 0),
	}
	c.tsoServerDiscovery = &tsoServerDiscovery{urls: make([]string, 0)}
	// Start with the default keyspace group. The actual keyspace group, to which the keyspace belongs,
	// will be discovered later.
	c.defaultDiscoveryKey = fmt.Sprintf(tsoSvcDiscoveryFormat, c.clusterID, constants.DefaultKeyspaceGroupID)

	log.Info("created tso service discovery",
		zap.Uint64("cluster-id", c.clusterID),
		zap.Uint32("keyspace-id", keyspaceID),
		zap.String("default-discovery-key", c.defaultDiscoveryKey))

	return c
}

// Init initialize the concrete client underlying
func (c *tsoServiceDiscovery) Init() error {
	log.Info("initializing tso service discovery",
		zap.Int("max-retry-times", c.option.MaxRetryTimes),
		zap.Duration("retry-interval", initRetryInterval))
	if err := c.retry(c.option.MaxRetryTimes, initRetryInterval, c.updateMember); err != nil {
		log.Error("failed to update member. initialization failed.", zap.Error(err))
		c.cancel()
		return err
	}
	c.wg.Add(1)
	go c.startCheckMemberLoop()
	return nil
}

func (c *tsoServiceDiscovery) retry(
	maxRetryTimes int, retryInterval time.Duration, f func() error,
) error {
	var err error
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	for range maxRetryTimes {
		if err = f(); err == nil {
			return nil
		}
		select {
		case <-c.ctx.Done():
			return err
		case <-ticker.C:
		}
	}
	return errors.WithStack(err)
}

// Close releases all resources
func (c *tsoServiceDiscovery) Close() {
	if c == nil {
		return
	}
	log.Info("closing tso service discovery")

	c.cancel()
	c.wg.Wait()

	c.clientConns.Range(func(key, cc any) bool {
		if err := cc.(*grpc.ClientConn).Close(); err != nil {
			log.Error("[tso] failed to close gRPC clientConn", errs.ZapError(errs.ErrCloseGRPCConn, err))
		}
		c.clientConns.Delete(key)
		return true
	})

	log.Info("tso service discovery is closed")
}

func (c *tsoServiceDiscovery) startCheckMemberLoop() {
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
			log.Info("[tso] exit check member loop")
			return
		}
		// Make sure tsoQueryRetryMaxTimes * tsoQueryRetryInterval is far less than memberUpdateInterval,
		// so that we can speed up the process of tso service discovery when failover happens on the
		// tso service side and also ensures it won't call updateMember too frequently during normal time.
		if err := c.retry(tsoQueryRetryMaxTimes, tsoQueryRetryInterval, c.updateMember); err != nil {
			log.Error("[tso] failed to update member", errs.ZapError(err))
		}
	}
}

// GetClusterID returns the ID of the cluster
func (c *tsoServiceDiscovery) GetClusterID() uint64 {
	return c.clusterID
}

// GetKeyspaceID returns the ID of the keyspace
func (c *tsoServiceDiscovery) GetKeyspaceID() uint32 {
	return c.keyspaceID.Load()
}

// SetKeyspaceID sets the ID of the keyspace
func (c *tsoServiceDiscovery) SetKeyspaceID(keyspaceID uint32) {
	c.keyspaceID.Store(keyspaceID)
}

// GetKeyspaceGroupID returns the ID of the keyspace group. If the keyspace group is unknown,
// it returns the default keyspace group ID.
func (c *tsoServiceDiscovery) GetKeyspaceGroupID() uint32 {
	c.keyspaceGroupSD.RLock()
	defer c.keyspaceGroupSD.RUnlock()
	if c.keyspaceGroupSD.group == nil {
		return constants.DefaultKeyspaceGroupID
	}
	return c.keyspaceGroupSD.group.Id
}

// GetServiceURLs returns the URLs of the tso primary/secondary URL of this keyspace group.
// For testing use. It should only be called when the client is closed.
func (c *tsoServiceDiscovery) GetServiceURLs() []string {
	c.keyspaceGroupSD.RLock()
	defer c.keyspaceGroupSD.RUnlock()
	return c.keyspaceGroupSD.urls
}

// GetServingEndpointClientConn returns the grpc client connection of the serving endpoint
// which is the primary in a primary/secondary configured cluster.
func (c *tsoServiceDiscovery) GetServingEndpointClientConn() *grpc.ClientConn {
	if cc, ok := c.clientConns.Load(c.getPrimaryURL()); ok {
		return cc.(*grpc.ClientConn)
	}
	return nil
}

// GetClientConns returns the mapping {URL -> a gRPC connection}
func (c *tsoServiceDiscovery) GetClientConns() *sync.Map {
	return &c.clientConns
}

// GetServingURL returns the serving endpoint which is the primary in a
// primary/secondary configured cluster.
func (c *tsoServiceDiscovery) GetServingURL() string {
	return c.getPrimaryURL()
}

// GetBackupURLs gets the URLs of the current reachable and healthy
// backup service endpoints. Backup service endpoints are secondaries in
// a primary/secondary configured cluster.
func (c *tsoServiceDiscovery) GetBackupURLs() []string {
	return c.getSecondaryURLs()
}

// GetOrCreateGRPCConn returns the corresponding grpc client connection of the given URL.
func (c *tsoServiceDiscovery) GetOrCreateGRPCConn(url string) (*grpc.ClientConn, error) {
	return grpcutil.GetOrCreateGRPCConn(c.ctx, &c.clientConns, url, c.tlsCfg, c.option.GRPCDialOptions...)
}

// ScheduleCheckMemberChanged is used to trigger a check to see if there is any change in service endpoints.
func (c *tsoServiceDiscovery) ScheduleCheckMemberChanged() {
	select {
	case c.checkMembershipCh <- struct{}{}:
	default:
	}
}

// CheckMemberChanged Immediately check if there is any membership change among the primary/secondaries in
// a primary/secondary configured cluster.
func (c *tsoServiceDiscovery) CheckMemberChanged() error {
	if err := c.serviceDiscovery.CheckMemberChanged(); err != nil {
		log.Warn("[tso] failed to check member changed", errs.ZapError(err))
	}
	if err := c.retry(tsoQueryRetryMaxTimes, tsoQueryRetryInterval, c.updateMember); err != nil {
		log.Error("[tso] failed to update member", errs.ZapError(err))
		return err
	}
	return nil
}

// ExecAndAddLeaderSwitchedCallback executes the callback once and adds it to the callback list then.
func (c *tsoServiceDiscovery) ExecAndAddLeaderSwitchedCallback(callback LeaderSwitchedCallbackFunc) {
	url := c.getPrimaryURL()
	if len(url) > 0 {
		if err := callback(url); err != nil {
			log.Error("[tso] failed to call back when tso global service url update", zap.String("url", url), errs.ZapError(err))
		}
	}
	c.tsoLeaderUpdatedCb = callback
}

// AddLeaderSwitchedCallback adds callbacks which will be called when the primary in
// a primary/secondary configured cluster is switched.
func (*tsoServiceDiscovery) AddLeaderSwitchedCallback(LeaderSwitchedCallbackFunc) {}

// AddMembersChangedCallback adds callbacks which will be called when any primary/secondary
// in a primary/secondary configured cluster is changed.
func (*tsoServiceDiscovery) AddMembersChangedCallback(func()) {}

// GetServiceClient implements ServiceDiscovery
func (c *tsoServiceDiscovery) GetServiceClient() ServiceClient {
	return c.serviceDiscovery.GetServiceClient()
}

// GetServiceClientByKind implements ServiceDiscovery
func (c *tsoServiceDiscovery) GetServiceClientByKind(kind APIKind) ServiceClient {
	return c.serviceDiscovery.GetServiceClientByKind(kind)
}

// GetAllServiceClients implements ServiceDiscovery
func (c *tsoServiceDiscovery) GetAllServiceClients() []ServiceClient {
	return c.serviceDiscovery.GetAllServiceClients()
}

// getPrimaryURL returns the primary URL.
func (c *tsoServiceDiscovery) getPrimaryURL() string {
	c.keyspaceGroupSD.RLock()
	defer c.keyspaceGroupSD.RUnlock()
	return c.keyspaceGroupSD.primaryURL
}

// getSecondaryURLs returns the secondary URLs.
func (c *tsoServiceDiscovery) getSecondaryURLs() []string {
	c.keyspaceGroupSD.RLock()
	defer c.keyspaceGroupSD.RUnlock()
	return c.keyspaceGroupSD.secondaryURLs
}

func (c *tsoServiceDiscovery) afterPrimarySwitched(oldPrimary, newPrimary string) error {
	// Run callbacks
	if c.tsoLeaderUpdatedCb != nil {
		if err := c.tsoLeaderUpdatedCb(newPrimary); err != nil {
			return err
		}
	}
	log.Info("[tso] switch primary",
		zap.String("new-primary", newPrimary),
		zap.String("old-primary", oldPrimary))
	return nil
}

// hasKeyspaceGroupIDConfig checks if the keyspace has been assigned a keyspace group
// by checking if tso_keyspace_group_id is present in its config.
// Returns true if the keyspace has tso_keyspace_group_id configured, false otherwise.
// If keyspaceMeta is nil (not passed from upper layer), returns false.
func (c *tsoServiceDiscovery) hasKeyspaceGroupIDConfig() bool {
	if c.keyspaceMeta == nil || c.keyspaceMeta.Config == nil {
		return false
	}

	// Check if tso_keyspace_group_id exists in config
	_, exists := c.keyspaceMeta.Config[constants.TSOKeyspaceGroupIDKey]
	return exists
}

// checkAndHandleFallbackInMicroserviceMode checks if fallback is allowed when TSO server is unavailable
// in microservice mode. Returns an error if fallback is not allowed, nil otherwise.
// Only keyspaces with tso_keyspace_group_id in their config should not fallback.
// This is to support backward compatibility for keyspaces that have not been assigned
// to any keyspace group yet (they should still use the default group 0).
func (c *tsoServiceDiscovery) checkAndHandleFallbackInMicroserviceMode(keyspaceID uint32) error {
	clusterInfo, err := c.serviceDiscovery.(*serviceDiscovery).getClusterInfo(
		c.ctx, c.serviceDiscovery.GetServingURL(), UpdateMemberTimeout)
	if err != nil {
		log.Warn("[tso] failed to get cluster info to check service mode",
			zap.Uint32("keyspace-id", keyspaceID),
			errs.ZapError(err))
		return err
	}

	// If we are in API_SVC_MODE (microservice mode), check if fallback is allowed
	if len(clusterInfo.ServiceModes) > 0 && clusterInfo.ServiceModes[0] == pdpb.ServiceMode_API_SVC_MODE {
		// Check if the keyspace has been assigned a keyspace group
		// Only keyspaces with tso_keyspace_group_id configured should not fallback
		if c.hasKeyspaceGroupIDConfig() {
			// This keyspace has been assigned to a keyspace group, don't fallback
			log.Warn("[tso] in microservice mode but no TSO server available - cannot fallback to group 0",
				zap.Uint32("keyspace-id", keyspaceID),
				zap.String("service-mode", clusterInfo.ServiceModes[0].String()))
			return errors.New("no TSO microservice available in microservice mode")
		}
		// Keyspace doesn't have tso_keyspace_group_id config, allow fallback
		log.Info("[tso] keyspace has no tso_keyspace_group_id config, allowing fallback to group 0",
			zap.Uint32("keyspace-id", keyspaceID),
			zap.String("service-mode", clusterInfo.ServiceModes[0].String()))
	}

	return nil
}

func (c *tsoServiceDiscovery) updateMember() error {
	// The keyspace membership or the primary serving URL of the keyspace group, to which this
	// keyspace belongs, might have been changed. We need to query tso servers to get the latest info.
	tsoServerURL, err := c.getTSOServer(c.serviceDiscovery)
	if err != nil {
		log.Error("[tso] failed to get tso server", errs.ZapError(err))
		return err
	}

	keyspaceID := c.GetKeyspaceID()
	var keyspaceGroup *tsopb.KeyspaceGroup
	if len(tsoServerURL) > 0 {
		keyspaceGroup, err = c.findGroupByKeyspaceID(keyspaceID, tsoServerURL, UpdateMemberTimeout)
		if err != nil {
			log.Error("[tso] failed to find the keyspace group",
				zap.Uint32("keyspace-id-in-request", keyspaceID),
				zap.String("tso-server-url", tsoServerURL),
				errs.ZapError(err))
			return err
		}
	} else {
		// Check the current service mode from PD to determine if fallback is appropriate
		// Note: tsoServiceDiscovery is only created in API_SVC_MODE (microservice mode)
		// If we are in API_SVC_MODE and tsoServerURL is empty, it means all TSO microservices
		// are unavailable. In this case, falling back to default group may cause issues like
		// timestamp fallback (issue #6770), so we should return an error instead.
		if err := c.checkAndHandleFallbackInMicroserviceMode(keyspaceID); err != nil {
			return err
		}

		// Only fallback to legacy path if we are in PD_SVC_MODE
		// This means the server hasn't been upgraded to the version that supports
		// independent TSO microservice, and we should use the PD's built-in TSO (group 0)
		c.printFallbackLogOnce.Do(func() {
			log.Warn("[tso] no tso server URL found,"+
				" fallback to the legacy path to discover from etcd directly",
				zap.Uint32("keyspace-id-in-request", keyspaceID),
				zap.String("tso-server-url", tsoServerURL),
				zap.String("discovery-key", c.defaultDiscoveryKey))
		})

		// Inject a failpoint to verify that in TSO MCS mode, we should NOT reach discoverWithLegacyPath
		// This failpoint is used in tests to ensure proper code path execution
		failpoint.Inject("assertNotReachLegacyPath", func(val failpoint.Value) {
			if shouldPanic, ok := val.(bool); ok && shouldPanic {
				panic("BUG: In TSO MCS mode, should not fallback to discoverWithLegacyPath when TSO server is temporarily unavailable")
			}
		})

		urls, err := c.discoverWithLegacyPath()
		if err != nil {
			return err
		}
		if len(urls) == 0 {
			return errors.New("no tso server url found")
		}
		members := make([]*tsopb.KeyspaceGroupMember, 0, len(urls))
		for _, url := range urls {
			members = append(members, &tsopb.KeyspaceGroupMember{Address: url})
		}
		members[0].IsPrimary = true
		keyspaceGroup = &tsopb.KeyspaceGroup{
			Id:      constants.DefaultKeyspaceGroupID,
			Members: members,
		}
	}

	oldGroupID := c.GetKeyspaceGroupID()
	if oldGroupID != keyspaceGroup.Id {
		log.Info("[tso] the keyspace group changed",
			zap.Uint32("keyspace-id", keyspaceID),
			zap.Uint32("new-keyspace-group-id", keyspaceGroup.Id),
			zap.Uint32("old-keyspace-group-id", oldGroupID))
	}

	// Initialize the serving URL from the returned keyspace group info.
	primaryURL := ""
	secondaryURLs := make([]string, 0)
	urls := make([]string, 0, len(keyspaceGroup.Members))
	for _, m := range keyspaceGroup.Members {
		urls = append(urls, m.Address)
		if m.IsPrimary {
			primaryURL = m.Address
		} else {
			secondaryURLs = append(secondaryURLs, m.Address)
		}
	}

	// If the primary URL is not empty, we need to create a grpc connection to it, and do it
	// out of the critical section of the keyspace group service discovery.
	if len(primaryURL) > 0 {
		if primarySwitched := !strings.EqualFold(primaryURL, c.getPrimaryURL()); primarySwitched {
			if _, err := c.GetOrCreateGRPCConn(primaryURL); err != nil {
				log.Warn("[tso] failed to connect the next primary",
					zap.Uint32("keyspace-id-in-request", keyspaceID),
					zap.String("tso-server-url", tsoServerURL),
					zap.String("next-primary", primaryURL), errs.ZapError(err))
				return err
			}
		}
	}

	oldPrimary, primarySwitched, _ :=
		c.keyspaceGroupSD.update(keyspaceGroup, primaryURL, secondaryURLs, urls)
	if primarySwitched {
		log.Info("[tso] updated keyspace group service discovery info",
			zap.Uint32("keyspace-id-in-request", keyspaceID),
			zap.String("tso-server-url", tsoServerURL),
			zap.String("keyspace-group-service", keyspaceGroup.String()))
		if err := c.afterPrimarySwitched(oldPrimary, primaryURL); err != nil {
			return err
		}
	}

	// Even if the primary URL is empty, we still updated other returned info above, including the
	// keyspace group info and the secondary url.
	if len(primaryURL) == 0 {
		return errors.New("no primary URL found")
	}

	return nil
}

// Query the keyspace group info from the tso server by the keyspace ID. The server side will return
// the info of the keyspace group to which this keyspace belongs.
func (c *tsoServiceDiscovery) findGroupByKeyspaceID(
	keyspaceID uint32, tsoSrvURL string, timeout time.Duration,
) (*tsopb.KeyspaceGroup, error) {
	failpoint.Inject("unexpectedCallOfFindGroupByKeyspaceID", func(val failpoint.Value) {
		keyspaceToCheck, ok := val.(int)
		if ok && keyspaceID == uint32(keyspaceToCheck) {
			panic("findGroupByKeyspaceID is called unexpectedly")
		}
	})
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	cc, err := c.GetOrCreateGRPCConn(tsoSrvURL)
	if err != nil {
		return nil, err
	}

	resp, err := tsopb.NewTSOClient(cc).FindGroupByKeyspaceID(
		ctx, &tsopb.FindGroupByKeyspaceIDRequest{
			Header: &tsopb.RequestHeader{
				ClusterId:       c.clusterID,
				KeyspaceId:      keyspaceID,
				KeyspaceGroupId: constants.DefaultKeyspaceGroupID,
			},
			KeyspaceId: keyspaceID,
		})
	if err != nil {
		attachErr := errors.Errorf("error:%s target:%s status:%s",
			err, cc.Target(), cc.GetState().String())
		return nil, errs.ErrClientFindGroupByKeyspaceID.Wrap(attachErr).GenWithStackByCause()
	}
	if resp.GetHeader().GetError() != nil {
		attachErr := errors.Errorf("error:%s target:%s status:%s",
			resp.GetHeader().GetError().String(), cc.Target(), cc.GetState().String())
		return nil, errs.ErrClientFindGroupByKeyspaceID.Wrap(attachErr).GenWithStackByCause()
	}
	if resp.KeyspaceGroup == nil {
		attachErr := errors.Errorf("error:%s target:%s status:%s",
			"no keyspace group found", cc.Target(), cc.GetState().String())
		return nil, errs.ErrClientFindGroupByKeyspaceID.Wrap(attachErr).GenWithStackByCause()
	}

	return resp.KeyspaceGroup, nil
}

func (c *tsoServiceDiscovery) getTSOServer(sd ServiceDiscovery) (string, error) {
	var (
		urls []string
		err  error
	)
	urls, err = sd.(*serviceDiscovery).discoverMicroservice(tsoService)
	if err != nil {
		return "", err
	}

	c.Lock()
	defer c.Unlock()
	t := c.tsoServerDiscovery
	failpoint.Inject("serverReturnsNoTSOAddrs", func() {
		if len(t.urls) == 0 {
			log.Info("[failpoint] injected error: server returns no tso URLs")
			urls = nil
		}
	})

	if len(urls) == 0 {
		// There is no error but no tso server url found, which means
		// the server side hasn't been upgraded to the version that
		// processes and returns GetClusterInfoResponse.TsoUrls. Return here
		// and handle the fallback logic outside of this function.
		return "", nil
	}

	if len(t.urls) == 0 || !EqualWithoutOrder(t.urls, urls) {
		log.Info("update tso server URLs", zap.Strings("urls", urls))
		t.urls = urls
		t.selectIdx = 0
	}

	// Pick a TSO server in a round-robin way.
	tsoServerURL := t.urls[t.selectIdx]
	t.selectIdx++
	t.selectIdx %= len(t.urls)

	return tsoServerURL, nil
}

func (c *tsoServiceDiscovery) discoverWithLegacyPath() ([]string, error) {
	resp, err := c.metacli.Get(c.ctx, []byte(c.defaultDiscoveryKey))
	if err != nil {
		log.Error("[tso] failed to get the keyspace serving endpoint",
			zap.String("discovery-key", c.defaultDiscoveryKey), errs.ZapError(err))
		return nil, err
	}
	if resp == nil || len(resp.Kvs) == 0 {
		log.Error("[tso] didn't find the keyspace serving endpoint",
			zap.String("primary-key", c.defaultDiscoveryKey))
		return nil, errs.ErrClientGetServingEndpoint
	} else if resp.Count > 1 {
		return nil, errs.ErrClientGetMultiResponse.FastGenByArgs(resp.Kvs)
	}

	value := resp.Kvs[0].Value
	primary := &tsopb.Participant{}
	if err := proto.Unmarshal(value, primary); err != nil {
		return nil, errs.ErrClientProtoUnmarshal.Wrap(err).GenWithStackByCause()
	}
	listenUrls := primary.GetListenUrls()
	if len(listenUrls) == 0 {
		log.Error("[tso] the keyspace serving endpoint list is empty",
			zap.String("discovery-key", c.defaultDiscoveryKey))
		return nil, errs.ErrClientGetServingEndpoint
	}
	return listenUrls, nil
}

// GetURLs returns the URLs of the TSO servers. Only used for testing.
func (c *tsoServiceDiscovery) GetURLs() []string {
	return c.urls
}

// EqualWithoutOrder checks if two slices are equal without considering the order.
func EqualWithoutOrder[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for _, item := range a {
		if !Contains(b, item) {
			return false
		}
	}
	return true
}

// Contains returns true if the given slice contains the value.
func Contains[T comparable](slice []T, value T) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}
