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

package pd

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
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/grpcutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	msServiceRootPath = "/ms"
	tsoServiceName    = "tso"
	// tsoSvcDiscoveryFormat defines the key prefix for keyspace group primary election.
	// The entire key is in the format of "/ms/<cluster-id>/tso/<group-id>/primary".
	// The <group-id> is 5 digits integer with leading zeros.
	tsoSvcDiscoveryFormat = msServiceRootPath + "/%d/" + tsoServiceName + "/%05d/primary"
	// initRetryInterval is the rpc retry interval during the initialization phase.
	initRetryInterval = time.Second
	// tsoQueryRetryMaxTimes is the max retry times for querying TSO.
	tsoQueryRetryMaxTimes = 10
	// tsoQueryRetryInterval is the retry interval for querying TSO.
	tsoQueryRetryInterval = 500 * time.Millisecond
)

var _ ServiceDiscovery = (*tsoServiceDiscovery)(nil)
var _ tsoAllocatorEventSource = (*tsoServiceDiscovery)(nil)

// keyspaceGroupSvcDiscovery is used for discovering the serving endpoints of the keyspace
// group to which the keyspace belongs
type keyspaceGroupSvcDiscovery struct {
	sync.RWMutex
	group *tsopb.KeyspaceGroup
	// primaryAddr is the primary serving address
	primaryAddr string
	// secondaryAddrs are TSO secondary serving addresses
	secondaryAddrs []string
	// addrs are the primary/secondary serving addresses
	addrs []string
}

func (k *keyspaceGroupSvcDiscovery) update(
	keyspaceGroup *tsopb.KeyspaceGroup,
	newPrimaryAddr string,
	secondaryAddrs, addrs []string,
) (oldPrimaryAddr string, primarySwitched, secondaryChanged bool) {
	k.Lock()
	defer k.Unlock()

	// If the new primary address is empty, we don't switch the primary address.
	oldPrimaryAddr = k.primaryAddr
	if len(newPrimaryAddr) > 0 {
		primarySwitched = !strings.EqualFold(oldPrimaryAddr, newPrimaryAddr)
		k.primaryAddr = newPrimaryAddr
	}

	if !reflect.DeepEqual(k.secondaryAddrs, secondaryAddrs) {
		k.secondaryAddrs = secondaryAddrs
		secondaryChanged = true
	}

	k.group = keyspaceGroup
	k.addrs = addrs
	return
}

// tsoServerDiscovery is for discovering the serving endpoints of the TSO servers
// TODO: dynamically update the TSO server addresses in the case of TSO server failover
// and scale-out/in.
type tsoServerDiscovery struct {
	sync.RWMutex
	addrs []string
	// used for round-robin load balancing
	selectIdx int
	// failureCount counts the consecutive failures for communicating with the tso servers
	failureCount int
}

func (t *tsoServerDiscovery) countFailure() bool {
	t.Lock()
	defer t.Unlock()
	t.failureCount++
	return t.failureCount >= len(t.addrs)
}

func (t *tsoServerDiscovery) resetFailure() {
	t.Lock()
	defer t.Unlock()
	t.failureCount = 0
}

// tsoServiceDiscovery is the service discovery client of the independent TSO service

type tsoServiceDiscovery struct {
	metacli         MetaStorageClient
	apiSvcDiscovery ServiceDiscovery
	clusterID       uint64
	keyspaceID      atomic.Uint32

	// defaultDiscoveryKey is the etcd path used for discovering the serving endpoints of
	// the default keyspace group
	defaultDiscoveryKey string
	// tsoServersDiscovery is for discovering the serving endpoints of the TSO servers
	*tsoServerDiscovery

	// keyspaceGroupSD is for discovering the serving endpoints of the keyspace group
	keyspaceGroupSD *keyspaceGroupSvcDiscovery

	// addr -> a gRPC connection
	clientConns sync.Map // Store as map[string]*grpc.ClientConn

	// localAllocPrimariesUpdatedCb will be called when the local tso allocator primary list is updated.
	// The input is a map {DC Location -> Leader Addr}
	localAllocPrimariesUpdatedCb tsoLocalServAddrsUpdatedFunc
	// globalAllocPrimariesUpdatedCb will be called when the local tso allocator primary list is updated.
	globalAllocPrimariesUpdatedCb tsoGlobalServAddrUpdatedFunc

	checkMembershipCh chan struct{}

	ctx                  context.Context
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
	printFallbackLogOnce sync.Once

	tlsCfg *tls.Config

	// Client option.
	option *option
}

// newTSOServiceDiscovery returns a new client-side service discovery for the independent TSO service.
func newTSOServiceDiscovery(
	ctx context.Context, metacli MetaStorageClient, apiSvcDiscovery ServiceDiscovery,
	clusterID uint64, keyspaceID uint32, tlsCfg *tls.Config, option *option,
) ServiceDiscovery {
	ctx, cancel := context.WithCancel(ctx)
	c := &tsoServiceDiscovery{
		ctx:               ctx,
		cancel:            cancel,
		metacli:           metacli,
		apiSvcDiscovery:   apiSvcDiscovery,
		clusterID:         clusterID,
		tlsCfg:            tlsCfg,
		option:            option,
		checkMembershipCh: make(chan struct{}, 1),
	}
	c.keyspaceID.Store(keyspaceID)
	c.keyspaceGroupSD = &keyspaceGroupSvcDiscovery{
		primaryAddr:    "",
		secondaryAddrs: make([]string, 0),
		addrs:          make([]string, 0),
	}
	c.tsoServerDiscovery = &tsoServerDiscovery{addrs: make([]string, 0)}
	// Start with the default keyspace group. The actual keyspace group, to which the keyspace belongs,
	// will be discovered later.
	c.defaultDiscoveryKey = fmt.Sprintf(tsoSvcDiscoveryFormat, clusterID, defaultKeySpaceGroupID)

	log.Info("created tso service discovery",
		zap.Uint64("cluster-id", clusterID),
		zap.Uint32("keyspace-id", keyspaceID),
		zap.String("default-discovery-key", c.defaultDiscoveryKey))

	return c
}

// Init initialize the concrete client underlying
func (c *tsoServiceDiscovery) Init() error {
	log.Info("initializing tso service discovery",
		zap.Int("max-retry-times", c.option.maxRetryTimes),
		zap.Duration("retry-interval", initRetryInterval))
	if err := c.retry(c.option.maxRetryTimes, initRetryInterval, c.updateMember); err != nil {
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
	for i := 0; i < maxRetryTimes; i++ {
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
	ticker := time.NewTicker(memberUpdateInterval)
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

// GetKeyspaceGroupID returns the ID of the keyspace group. If the keyspace group is unknown,
// it returns the default keyspace group ID.
func (c *tsoServiceDiscovery) GetKeyspaceGroupID() uint32 {
	c.keyspaceGroupSD.RLock()
	defer c.keyspaceGroupSD.RUnlock()
	if c.keyspaceGroupSD.group == nil {
		return defaultKeySpaceGroupID
	}
	return c.keyspaceGroupSD.group.Id
}

// GetServiceURLs returns the URLs of the tso primary/secondary addresses of this keyspace group.
// For testing use. It should only be called when the client is closed.
func (c *tsoServiceDiscovery) GetServiceURLs() []string {
	c.keyspaceGroupSD.RLock()
	defer c.keyspaceGroupSD.RUnlock()
	return c.keyspaceGroupSD.addrs
}

// GetServingAddr returns the grpc client connection of the serving endpoint
// which is the primary in a primary/secondary configured cluster.
func (c *tsoServiceDiscovery) GetServingEndpointClientConn() *grpc.ClientConn {
	if cc, ok := c.clientConns.Load(c.getPrimaryAddr()); ok {
		return cc.(*grpc.ClientConn)
	}
	return nil
}

// GetClientConns returns the mapping {addr -> a gRPC connection}
func (c *tsoServiceDiscovery) GetClientConns() *sync.Map {
	return &c.clientConns
}

// GetServingAddr returns the serving endpoint which is the primary in a
// primary/secondary configured cluster.
func (c *tsoServiceDiscovery) GetServingAddr() string {
	return c.getPrimaryAddr()
}

// GetBackupAddrs gets the addresses of the current reachable and healthy
// backup service endpoints. Backup service endpoints are secondaries in
// a primary/secondary configured cluster.
func (c *tsoServiceDiscovery) GetBackupAddrs() []string {
	return c.getSecondaryAddrs()
}

// GetOrCreateGRPCConn returns the corresponding grpc client connection of the given addr.
func (c *tsoServiceDiscovery) GetOrCreateGRPCConn(addr string) (*grpc.ClientConn, error) {
	return grpcutil.GetOrCreateGRPCConn(c.ctx, &c.clientConns, addr, c.tlsCfg, c.option.gRPCDialOptions...)
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
	c.apiSvcDiscovery.CheckMemberChanged()
	if err := c.retry(tsoQueryRetryMaxTimes, tsoQueryRetryInterval, c.updateMember); err != nil {
		log.Error("[tso] failed to update member", errs.ZapError(err))
		return err
	}
	return nil
}

// AddServingAddrSwitchedCallback adds callbacks which will be called when the primary in
// a primary/secondary configured cluster is switched.
func (c *tsoServiceDiscovery) AddServingAddrSwitchedCallback(callbacks ...func()) {
}

// AddServiceAddrsSwitchedCallback adds callbacks which will be called when any primary/secondary
// in a primary/secondary configured cluster is changed.
func (c *tsoServiceDiscovery) AddServiceAddrsSwitchedCallback(callbacks ...func()) {
}

// SetTSOLocalServAddrsUpdatedCallback adds a callback which will be called when the local tso
// allocator leader list is updated.
func (c *tsoServiceDiscovery) SetTSOLocalServAddrsUpdatedCallback(callback tsoLocalServAddrsUpdatedFunc) {
	c.localAllocPrimariesUpdatedCb = callback
}

// SetTSOGlobalServAddrUpdatedCallback adds a callback which will be called when the global tso
// allocator leader is updated.
func (c *tsoServiceDiscovery) SetTSOGlobalServAddrUpdatedCallback(callback tsoGlobalServAddrUpdatedFunc) {
	addr := c.getPrimaryAddr()
	if len(addr) > 0 {
		callback(addr)
	}
	c.globalAllocPrimariesUpdatedCb = callback
}

// GetServiceClient implements ServiceDiscovery
func (c *tsoServiceDiscovery) GetServiceClient() ServiceClient {
	return c.apiSvcDiscovery.GetServiceClient()
}

// GetAllServiceClients implements ServiceDiscovery
func (c *tsoServiceDiscovery) GetAllServiceClients() []ServiceClient {
	return c.apiSvcDiscovery.GetAllServiceClients()
}

// getPrimaryAddr returns the primary address.
func (c *tsoServiceDiscovery) getPrimaryAddr() string {
	c.keyspaceGroupSD.RLock()
	defer c.keyspaceGroupSD.RUnlock()
	return c.keyspaceGroupSD.primaryAddr
}

// getSecondaryAddrs returns the secondary addresses.
func (c *tsoServiceDiscovery) getSecondaryAddrs() []string {
	c.keyspaceGroupSD.RLock()
	defer c.keyspaceGroupSD.RUnlock()
	return c.keyspaceGroupSD.secondaryAddrs
}

func (c *tsoServiceDiscovery) afterPrimarySwitched(oldPrimary, newPrimary string) error {
	// Run callbacks
	if c.globalAllocPrimariesUpdatedCb != nil {
		if err := c.globalAllocPrimariesUpdatedCb(newPrimary); err != nil {
			return err
		}
	}
	log.Info("[tso] switch primary",
		zap.String("new-primary", newPrimary),
		zap.String("old-primary", oldPrimary))
	return nil
}

func (c *tsoServiceDiscovery) updateMember() error {
	// The keyspace membership or the primary serving address of the keyspace group, to which this
	// keyspace belongs, might have been changed. We need to query tso servers to get the latest info.
	tsoServerAddr, err := c.getTSOServer(c.apiSvcDiscovery)
	if err != nil {
		log.Error("[tso] failed to get tso server", errs.ZapError(err))
		return err
	}

	keyspaceID := c.GetKeyspaceID()
	var keyspaceGroup *tsopb.KeyspaceGroup
	if len(tsoServerAddr) > 0 {
		keyspaceGroup, err = c.findGroupByKeyspaceID(keyspaceID, tsoServerAddr, updateMemberTimeout)
		if err != nil {
			if c.tsoServerDiscovery.countFailure() {
				log.Error("[tso] failed to find the keyspace group",
					zap.Uint32("keyspace-id-in-request", keyspaceID),
					zap.String("tso-server-addr", tsoServerAddr),
					errs.ZapError(err))
			}
			return err
		}
		c.tsoServerDiscovery.resetFailure()
	} else {
		// There is no error but no tso server address found, which means
		// the server side hasn't been upgraded to the version that
		// processes and returns GetClusterInfoResponse.TsoUrls. In this case,
		// we fall back to the old way of discovering the tso primary addresses
		// from etcd directly.
		c.printFallbackLogOnce.Do(func() {
			log.Warn("[tso] no tso server address found,"+
				" fallback to the legacy path to discover from etcd directly",
				zap.Uint32("keyspace-id-in-request", keyspaceID),
				zap.String("tso-server-addr", tsoServerAddr),
				zap.String("discovery-key", c.defaultDiscoveryKey))
		})
		addrs, err := c.discoverWithLegacyPath()
		if err != nil {
			return err
		}
		if len(addrs) == 0 {
			return errors.New("no tso server address found")
		}
		members := make([]*tsopb.KeyspaceGroupMember, 0, len(addrs))
		for _, addr := range addrs {
			members = append(members, &tsopb.KeyspaceGroupMember{Address: addr})
		}
		members[0].IsPrimary = true
		keyspaceGroup = &tsopb.KeyspaceGroup{
			Id:      defaultKeySpaceGroupID,
			Members: members,
		}
	}

	oldGroupID := c.GetKeyspaceGroupID()
	if oldGroupID != keyspaceGroup.Id {
		log.Info("[tso] the keyspace group changed",
			zap.Uint32("keyspace-id", keyspaceGroup.Id),
			zap.Uint32("new-keyspace-group-id", keyspaceGroup.Id),
			zap.Uint32("old-keyspace-group-id", oldGroupID))
	}

	// Initialize the serving addresses from the returned keyspace group info.
	primaryAddr := ""
	secondaryAddrs := make([]string, 0)
	addrs := make([]string, 0, len(keyspaceGroup.Members))
	for _, m := range keyspaceGroup.Members {
		addrs = append(addrs, m.Address)
		if m.IsPrimary {
			primaryAddr = m.Address
		} else {
			secondaryAddrs = append(secondaryAddrs, m.Address)
		}
	}

	// If the primary address is not empty, we need to create a grpc connection to it, and do it
	// out of the critical section of the keyspace group service discovery.
	if len(primaryAddr) > 0 {
		if primarySwitched := !strings.EqualFold(primaryAddr, c.getPrimaryAddr()); primarySwitched {
			if _, err := c.GetOrCreateGRPCConn(primaryAddr); err != nil {
				log.Warn("[tso] failed to connect the next primary",
					zap.Uint32("keyspace-id-in-request", keyspaceID),
					zap.String("tso-server-addr", tsoServerAddr),
					zap.String("next-primary", primaryAddr), errs.ZapError(err))
				return err
			}
		}
	}

	oldPrimary, primarySwitched, _ :=
		c.keyspaceGroupSD.update(keyspaceGroup, primaryAddr, secondaryAddrs, addrs)
	if primarySwitched {
		log.Info("[tso] updated keyspace group service discovery info",
			zap.Uint32("keyspace-id-in-request", keyspaceID),
			zap.String("tso-server-addr", tsoServerAddr),
			zap.String("keyspace-group-service", keyspaceGroup.String()))
		if err := c.afterPrimarySwitched(oldPrimary, primaryAddr); err != nil {
			return err
		}
	}

	// Even if the primary address is empty, we still updated other returned info above, including the
	// keyspace group info and the secondary addresses.
	if len(primaryAddr) == 0 {
		return errors.New("no primary address found")
	}

	return nil
}

// Query the keyspace group info from the tso server by the keyspace ID. The server side will return
// the info of the keyspace group to which this keyspace belongs.
func (c *tsoServiceDiscovery) findGroupByKeyspaceID(
	keyspaceID uint32, tsoSrvAddr string, timeout time.Duration,
) (*tsopb.KeyspaceGroup, error) {
	failpoint.Inject("unexpectedCallOfFindGroupByKeyspaceID", func(val failpoint.Value) {
		keyspaceToCheck, ok := val.(int)
		if ok && keyspaceID == uint32(keyspaceToCheck) {
			panic("findGroupByKeyspaceID is called unexpectedly")
		}
	})
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	cc, err := c.GetOrCreateGRPCConn(tsoSrvAddr)
	if err != nil {
		return nil, err
	}

	resp, err := tsopb.NewTSOClient(cc).FindGroupByKeyspaceID(
		ctx, &tsopb.FindGroupByKeyspaceIDRequest{
			Header: &tsopb.RequestHeader{
				ClusterId:       c.clusterID,
				KeyspaceId:      keyspaceID,
				KeyspaceGroupId: defaultKeySpaceGroupID,
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
	c.Lock()
	defer c.Unlock()

	var (
		addrs []string
		err   error
	)
	t := c.tsoServerDiscovery
	if len(t.addrs) == 0 || t.failureCount == len(t.addrs) {
		addrs, err = sd.(*pdServiceDiscovery).discoverMicroservice(tsoService)
		if err != nil {
			return "", err
		}
		failpoint.Inject("serverReturnsNoTSOAddrs", func() {
			log.Info("[failpoint] injected error: server returns no tso addrs")
			addrs = nil
		})
		if len(addrs) == 0 {
			// There is no error but no tso server address found, which means
			// the server side hasn't been upgraded to the version that
			// processes and returns GetClusterInfoResponse.TsoUrls. Return here
			// and handle the fallback logic outside of this function.
			return "", nil
		}

		log.Info("update tso server addresses", zap.Strings("addrs", addrs))

		t.addrs = addrs
		t.selectIdx = 0
		t.failureCount = 0
	}

	// Pick a TSO server in a round-robin way.
	tsoServerAddr := t.addrs[t.selectIdx]
	t.selectIdx++
	t.selectIdx %= len(t.addrs)

	return tsoServerAddr, nil
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
