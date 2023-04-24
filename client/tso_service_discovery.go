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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/grpcutil"
	"github.com/tikv/pd/client/tlsutil"
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
	// used for round-robin load balancing when communicating with serving addresses
	selectIdx int
	// failureCount counts the consecutive failures for communicating with serving addresses
	failureCount int
}

func (k *keyspaceGroupSvcDiscovery) update(
	keyspaceGroup *tsopb.KeyspaceGroup,
	newPrimaryAddr string,
	secondaryAddrs, addrs []string,
) (oldPrimaryAddr string, primarySwitched bool) {
	k.Lock()
	defer k.Unlock()

	// If the new primary address is empty, we don't switch the primary address.
	oldPrimaryAddr = k.primaryAddr
	if len(newPrimaryAddr) > 0 {
		primarySwitched = !strings.EqualFold(oldPrimaryAddr, newPrimaryAddr)
		k.primaryAddr = newPrimaryAddr
	}

	k.group = keyspaceGroup
	k.secondaryAddrs = secondaryAddrs
	k.addrs = addrs

	return
}

func (k *keyspaceGroupSvcDiscovery) getServer() string {
	k.Lock()
	defer k.Unlock()
	// If we have tried all the serving addresses for this keyspace group, we should
	// return an empty address and let the caller to try other tso servers.
	if len(k.addrs) == 0 || k.failureCount >= len(k.addrs) {
		return ""
	}
	server := k.addrs[k.selectIdx]
	k.selectIdx = (k.selectIdx + 1) % len(k.addrs)
	return server
}

func (k *keyspaceGroupSvcDiscovery) countFailure(err error) {
	k.Lock()
	defer k.Unlock()
	if err != nil {
		k.failureCount++
	} else {
		// Clear all failures since we only count consecutive failures
		k.failureCount = 0
	}
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

func (t *tsoServerDiscovery) getTSOServer(sd ServiceDiscovery) (string, error) {
	t.Lock()
	defer t.Unlock()

	if len(t.addrs) == 0 || t.failureCount > 0 {
		addrs := sd.DiscoverMicroservice(tsoService)
		if len(addrs) == 0 {
			return "", errors.New("no tso server address found")
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

func (t *tsoServerDiscovery) countFailure(err error) {
	t.Lock()
	defer t.Unlock()
	if err != nil {
		t.failureCount++
	} else {
		// Clear all failures since we only count consecutive failures
		t.failureCount = 0
	}
}

// tsoServiceDiscovery is the service discovery client of the independent TSO service

type tsoServiceDiscovery struct {
	metacli         MetaStorageClient
	apiSvcDiscovery ServiceDiscovery
	clusterID       uint64
	keyspaceID      uint32

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

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	tlsCfg *tlsutil.TLSConfig

	// Client option.
	option *option
}

// newTSOServiceDiscovery returns a new client-side service discovery for the independent TSO service.
func newTSOServiceDiscovery(
	ctx context.Context, metacli MetaStorageClient, apiSvcDiscovery ServiceDiscovery,
	clusterID uint64, keyspaceID uint32, tlsCfg *tlsutil.TLSConfig, option *option,
) ServiceDiscovery {
	ctx, cancel := context.WithCancel(ctx)
	c := &tsoServiceDiscovery{
		ctx:               ctx,
		cancel:            cancel,
		metacli:           metacli,
		apiSvcDiscovery:   apiSvcDiscovery,
		keyspaceID:        keyspaceID,
		clusterID:         clusterID,
		tlsCfg:            tlsCfg,
		option:            option,
		checkMembershipCh: make(chan struct{}, 1),
	}
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
	for i := 0; i < maxRetryTimes; i++ {
		if err = f(); err == nil {
			return nil
		}
		select {
		case <-c.ctx.Done():
			return err
		case <-time.After(retryInterval):
		}
	}
	return errors.WithStack(err)
}

// Close releases all resources
func (c *tsoServiceDiscovery) Close() {
	log.Info("closing tso service discovery")

	c.cancel()
	c.wg.Wait()

	c.clientConns.Range(func(key, cc interface{}) bool {
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

	for {
		select {
		case <-c.checkMembershipCh:
		case <-time.After(memberUpdateInterval):
		case <-ctx.Done():
			log.Info("[tso] exit check member loop")
			return
		}
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
	return c.keyspaceID
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

// DiscoverServiceURLs discovers the microservice with the specified type and returns the server urls.
func (c *tsoServiceDiscovery) DiscoverMicroservice(svcType serviceType) []string {
	var urls []string

	switch svcType {
	case apiService:
	case tsoService:
		return c.apiSvcDiscovery.DiscoverMicroservice(tsoService)
	default:
		panic("invalid service type")
	}

	return urls
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

// Immediately check if there is any membership change among the primary/secondaries in
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

func (c *tsoServiceDiscovery) updateMember() (err error) {
	// The keyspace membership or the primary serving address of the keyspace group, to which this
	// keyspace belongs, might have been changed. We need to query tso servers to get the latest info.
	// If there are known healthy tso servers serving this keyspace, we start with them; otherwise,
	// we start with any healthy tso server.
	var keyspaceGroup *tsopb.KeyspaceGroup
	tsoServerAddr := c.keyspaceGroupSD.getServer()
	if len(tsoServerAddr) > 0 {
		keyspaceGroup, err = c.findGroupByKeyspaceID(c.keyspaceID, tsoServerAddr, updateMemberTimeout)
		c.keyspaceGroupSD.countFailure(err)
	} else {
		if tsoServerAddr, err = c.getTSOServer(c.apiSvcDiscovery); err != nil {
			return err
		}
		keyspaceGroup, err = c.findGroupByKeyspaceID(c.keyspaceID, tsoServerAddr, updateMemberTimeout)
		c.tsoServerDiscovery.countFailure(err)
	}
	if err != nil {
		return err
	}

	// Initialize the all types of serving addresses from the returned keyspace group info.
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
					zap.String("next-primary", primaryAddr), errs.ZapError(err))
				return err
			}
		}
	}

	oldPrimary, primarySwitched := c.keyspaceGroupSD.update(keyspaceGroup, primaryAddr, secondaryAddrs, addrs)
	if primarySwitched {
		if err := c.afterPrimarySwitched(oldPrimary, primaryAddr); err != nil {
			return err
		}
	}

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
