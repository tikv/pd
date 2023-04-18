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
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
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
	// tspSvcDiscoveryFormat defines the key prefix for keyspace group primary election.
	// The entire key is in the format of "/ms/<cluster-id>/tso/<group-id>/primary".
	// The <group-id> is 5 digits integer with leading zeros.
	tspSvcDiscoveryFormat = msServiceRootPath + "/%d/" + tsoServiceName + "/%05d/primary"
	// tsoRPCTimeout is the timeout for TSO RPC requests.
	tsoRPCTimeout = time.Second
)

var _ ServiceDiscovery = (*tsoServiceDiscovery)(nil)
var _ tsoAllocatorEventSource = (*tsoServiceDiscovery)(nil)

type keyspaceGroup struct {
	sync.RWMutex
	id    uint32
	group *tsopb.KeyspaceGroup
	// primaryAddr is the TSO primary address of this keyspace group
	primaryAddr string
	// secondaryAddrs are TSO secondary addresses of this keyspace group
	secondaryAddrs []string
	// allAddrs are all TSO addresses of this keyspace group
	allAddrs []string
}

// tsoServiceDiscovery is the service discovery client of the independent TSO service
type tsoServiceDiscovery struct {
	clusterID  uint64
	keyspaceID uint32
	apiSvcUrls []string
	// tsoSrvAddrs is the TSO server addresses
	tsoSrvAddrs []string
	// defaultDiscoveryKey is the etcd path used for discovering the serving endpoints of
	// the default keyspace group
	defaultDiscoveryKey string
	metacli             MetaStorageClient

	*keyspaceGroup

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
	ctx context.Context, metacli MetaStorageClient,
	clusterID uint64, keyspaceID uint32, apiSvcUrls []string, tlsCfg *tlsutil.TLSConfig, option *option,
) ServiceDiscovery {
	ctx, cancel := context.WithCancel(ctx)
	c := &tsoServiceDiscovery{
		ctx:               ctx,
		cancel:            cancel,
		metacli:           metacli,
		keyspaceID:        keyspaceID,
		clusterID:         clusterID,
		apiSvcUrls:        apiSvcUrls,
		tsoSrvAddrs:       make([]string, 0),
		tlsCfg:            tlsCfg,
		option:            option,
		checkMembershipCh: make(chan struct{}, 1),
	}
	// Start with the default keyspace group. The actual keyspace group, to which the keyspace belongs,
	// will be discovered later.
	c.keyspaceGroup = &keyspaceGroup{
		id:             defaultKeySpaceGroupID,
		primaryAddr:    "",
		secondaryAddrs: make([]string, 0),
		allAddrs:       make([]string, 0),
	}
	c.defaultDiscoveryKey = fmt.Sprintf(tspSvcDiscoveryFormat, clusterID, defaultKeySpaceGroupID)

	log.Info("created tso service discovery",
		zap.Uint64("cluster-id", clusterID),
		zap.Uint32("keyspace-id", keyspaceID),
		zap.String("default-discovery-key", c.defaultDiscoveryKey))

	return c
}

// Init initialize the concrete client underlying
func (c *tsoServiceDiscovery) Init() error {
	if err := c.initRetry(c.updateMember); err != nil {
		c.cancel()
		return err
	}
	c.wg.Add(1)
	go c.startCheckMemberLoop()
	return nil
}

func (c *tsoServiceDiscovery) initRetry(f func() error) error {
	var err error
	for i := 0; i < c.option.maxRetryTimes; i++ {
		if err = f(); err == nil {
			return nil
		}
		select {
		case <-c.ctx.Done():
			return err
		case <-time.After(time.Second):
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
		if err := c.updateMember(); err != nil {
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
	c.keyspaceGroup.RLock()
	defer c.keyspaceGroup.RUnlock()
	return c.keyspaceGroup.id
}

// GetURLs returns the URLs of the tso primary/secondary addresses of this keyspace group.
// For testing use. It should only be called when the client is closed.
func (c *tsoServiceDiscovery) GetURLs() []string {
	c.keyspaceGroup.RLock()
	defer c.keyspaceGroup.RUnlock()
	if c.keyspaceGroup == nil {
		return nil
	}
	return c.keyspaceGroup.allAddrs
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
	return c.updateMember()
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
	c.keyspaceGroup.RLock()
	defer c.keyspaceGroup.RUnlock()
	if c.keyspaceGroup == nil {
		return ""
	}
	return c.keyspaceGroup.primaryAddr
}

// getSecondaryAddrs returns the secondary addresses.
func (c *tsoServiceDiscovery) getSecondaryAddrs() []string {
	c.keyspaceGroup.RLock()
	defer c.keyspaceGroup.RUnlock()
	if c.keyspaceGroup == nil {
		return nil
	}
	return c.keyspaceGroup.secondaryAddrs
}

func (c *tsoServiceDiscovery) switchPrimary(primaryAddr string) error {
	oldPrimary := c.keyspaceGroup.primaryAddr
	if primaryAddr == oldPrimary {
		return nil
	}

	if _, err := c.GetOrCreateGRPCConn(primaryAddr); err != nil {
		log.Warn("[tso] failed to connect primary",
			zap.String("new-primary", primaryAddr), errs.ZapError(err))
		return err
	}
	// Set PD primary and Global TSO Allocator (which is also the PD primary)
	c.primaryAddr = primaryAddr
	// Run callbacks
	if c.globalAllocPrimariesUpdatedCb != nil {
		if err := c.globalAllocPrimariesUpdatedCb(primaryAddr); err != nil {
			return err
		}
	}
	log.Info("[tso] switch primary",
		zap.String("new-primary", primaryAddr),
		zap.String("old-primary", oldPrimary))
	return nil
}

func (c *tsoServiceDiscovery) updateMember() (err error) {
	if len(c.tsoSrvAddrs) == 0 {
		// TODO: discover all registered TSO servers instead of just the servers serving
		// the default keyspace group.
		c.tsoSrvAddrs, err = c.getDefaultGroupSvcAddrs()
		if err != nil {
			return err
		}
		if len(c.tsoSrvAddrs) == 0 {
			return errors.New("no tso server address found")
		}
	}

	// Randomly choose a TSO server to query the keyspace group to which the keyspace belongs.
	randIdx := rand.Intn(len(c.tsoSrvAddrs))
	kg, err := c.getKeyspaceGroup(c.tsoSrvAddrs[randIdx], defaultKeySpaceGroupID, tsoRPCTimeout)
	if err != nil {
		if !strings.Contains(err.Error(), "Unimplemented") {
			return err
		}

		log.Warn("[tso] the server doesn't support the method tsopb.FindGroupByKeyspaceID")

		// TODO: it's a hack way to solve the compatibility issue just in case the server side
		// doesn't support the method tsopb.FindGroupByKeyspaceID. We should remove this after
		// all maintained version supports the method.
		members := make([]*tsopb.KeyspaceGroupMember, 0, len(c.tsoSrvAddrs))
		if len(c.tsoSrvAddrs) > 0 {
			members = append(members, &tsopb.KeyspaceGroupMember{
				Address:   c.tsoSrvAddrs[0],
				IsPrimary: true,
			})
		}

		for _, addr := range c.tsoSrvAddrs[1:] {
			members = append(members, &tsopb.KeyspaceGroupMember{
				Address: addr,
			})
		}

		kg = &tsopb.KeyspaceGroup{
			Id:      defaultKeySpaceGroupID,
			Members: members,
		}
	}
	if kg == nil {
		return errors.New("no keyspace group found")
	}
	if len(kg.Members) == 0 {
		return errors.New("no keyspace group member found")
	}

	c.keyspaceGroup.Lock()
	defer c.keyspaceGroup.Unlock()

	c.keyspaceGroup.group = kg
	c.keyspaceGroup.id = kg.Id
	c.keyspaceGroup.primaryAddr = ""
	c.keyspaceGroup.secondaryAddrs = make([]string, 0)
	c.keyspaceGroup.allAddrs = make([]string, 0, len(kg.Members))
	for _, m := range kg.Members {
		c.keyspaceGroup.allAddrs = append(c.keyspaceGroup.allAddrs, m.Address)
		if m.IsPrimary {
			c.keyspaceGroup.primaryAddr = m.Address
		} else {
			c.keyspaceGroup.secondaryAddrs = append(c.keyspaceGroup.secondaryAddrs, m.Address)
		}
	}

	if len(c.keyspaceGroup.primaryAddr) == 0 {
		return errors.New("no primary address found")
	}
	c.switchPrimary(c.keyspaceGroup.primaryAddr)

	return nil
}

func (c *tsoServiceDiscovery) getDefaultGroupSvcAddrs() ([]string, error) {
	resp, err := c.metacli.Get(c.ctx, []byte(c.defaultDiscoveryKey))
	if err != nil {
		log.Error("[tso] failed to get the keyspace serving endpoint",
			zap.String("primary-key", c.defaultDiscoveryKey), errs.ZapError(err))
		return nil, err
	}

	if resp == nil || len(resp.Kvs) == 0 {
		log.Error("[tso] didn't find the keyspace serving endpoint", zap.String("primary-key", c.defaultDiscoveryKey))
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
		log.Error("[tso] the keyspace serving endpoint list is empty", zap.String("primary-key", c.defaultDiscoveryKey))
		return nil, errs.ErrClientGetServingEndpoint
	}
	return listenUrls, nil
}

func (c *tsoServiceDiscovery) getKeyspaceGroup(
	url string, keyspaceGroupID uint32, timeout time.Duration,
) (*tsopb.KeyspaceGroup, error) {
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	cc, err := c.GetOrCreateGRPCConn(url)
	if err != nil {
		return nil, err
	}
	resp, err := tsopb.NewTSOClient(cc).FindGroupByKeyspaceID(
		ctx, &tsopb.FindGroupByKeyspaceIDRequest{
			Header: &tsopb.RequestHeader{
				ClusterId:       c.clusterID,
				KeyspaceId:      c.keyspaceID,
				KeyspaceGroupId: keyspaceGroupID,
			},
			KeyspaceId: c.keyspaceID,
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
			"empty keyspace group", cc.Target(), cc.GetState().String())
		return nil, errs.ErrClientFindGroupByKeyspaceID.Wrap(attachErr).GenWithStackByCause()
	}
	return resp.KeyspaceGroup, nil
}
