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
	"sync"
	"sync/atomic"
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
)

var _ ServiceDiscovery = (*tsoServiceDiscovery)(nil)
var _ tsoAllocatorEventSource = (*tsoServiceDiscovery)(nil)

// tsoServiceDiscovery is the service discovery client of the independent TSO service
type tsoServiceDiscovery struct {
	clusterID  uint64
	keyspaceID uint32
	urls       atomic.Value // Store as []string
	// primary key is the etcd path used for discoverying the serving endpoint of this keyspace
	primaryKey string
	// TSO Primary URL
	primary atomic.Value // Store as string
	// TSO Secondary URLs
	secondaries atomic.Value // Store as []string
	metacli     MetaStorageClient

	// addr -> a gRPC connection
	clientConns sync.Map // Store as map[string]*grpc.ClientConn

	// localAllocPrimariesUpdatedCb will be called when the local tso allocator primary list is updated.
	// The input is a map {DC Localtion -> Leader Addr}
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
func newTSOServiceDiscovery(ctx context.Context, cancel context.CancelFunc, metacli MetaStorageClient,
	clusterID uint64, keyspaceID uint32, urls []string, tlsCfg *tlsutil.TLSConfig, option *option) ServiceDiscovery {
	c := &tsoServiceDiscovery{
		ctx:               ctx,
		cancel:            cancel,
		metacli:           metacli,
		keyspaceID:        keyspaceID,
		clusterID:         clusterID,
		primaryKey:        fmt.Sprintf(tspSvcDiscoveryFormat, clusterID, keyspaceID),
		tlsCfg:            tlsCfg,
		option:            option,
		checkMembershipCh: make(chan struct{}, 1),
	}
	c.urls.Store(urls)

	log.Info("created tso service discovery", zap.String("discovery-key", c.primaryKey))

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

// GetURLs returns the URLs of the servers.
// For testing use. It should only be called when the client is closed.
func (c *tsoServiceDiscovery) GetURLs() []string {
	return c.urls.Load().([]string)
}

// GetServingAddr returns the grpc client connection of the serving endpoint
// which is the primary in a primary/secondary configured cluster.
func (c *tsoServiceDiscovery) GetServingEndpointClientConn() *grpc.ClientConn {
	if cc, ok := c.clientConns.Load(c.getPrimaryAddr()); ok {
		return cc.(*grpc.ClientConn)
	}
	return nil
}

// GetClientConns returns the mapping {addr -> a gRPC connectio}
func (c *tsoServiceDiscovery) GetClientConns() *sync.Map {
	return &c.clientConns
}

// GetServingAddr returns the serving endpoint which is the primary in a
// primary/secondary configured cluster.
func (c *tsoServiceDiscovery) GetServingAddr() string {
	return c.getPrimaryAddr()
}

// GetBackupAddrs gets the addresses of the current reachable and healthy
// backup service endpoints randomly. Backup service endpoints are secondaries in
// a primary/secondary configured cluster.
func (c *tsoServiceDiscovery) GetBackupAddrs() []string {
	return c.getSecondaryAddrs()
}

// GetOrCreateGRPCConn returns the corresponding grpc client connection of the given addr.
func (c *tsoServiceDiscovery) GetOrCreateGRPCConn(addr string) (*grpc.ClientConn, error) {
	return grpcutil.GetOrCreateGRPCConn(c.ctx, &c.clientConns, addr, c.tlsCfg, c.option.gRPCDialOptions...)
}

// ScheduleCheckMemberChanged is used to trigger a check to see if there is any change in ervice endpoints.
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
	primaryAddr := c.primary.Load()
	if primaryAddr == nil {
		return ""
	}
	return primaryAddr.(string)
}

// getSecondaryAddrs returns the secondary addresses.
func (c *tsoServiceDiscovery) getSecondaryAddrs() []string {
	secondaryAddrs := c.secondaries.Load()
	if secondaryAddrs == nil {
		return []string{}
	}
	return secondaryAddrs.([]string)
}

func (c *tsoServiceDiscovery) switchPrimary(addrs []string) error {
	// FIXME: How to safely compare primary urls? For now, only allows one client url.
	addr := addrs[0]
	oldPrimary := c.getPrimaryAddr()
	if addr == oldPrimary {
		return nil
	}

	if _, err := c.GetOrCreateGRPCConn(addr); err != nil {
		log.Warn("[tso] failed to connect primary", zap.String("primary", addr), errs.ZapError(err))
		return err
	}
	// Set PD primary and Global TSO Allocator (which is also the PD primary)
	c.primary.Store(addr)
	// Run callbacks
	if c.globalAllocPrimariesUpdatedCb != nil {
		if err := c.globalAllocPrimariesUpdatedCb(addr); err != nil {
			return err
		}
	}
	log.Info("[tso] switch primary", zap.String("new-primary", addr), zap.String("old-primary", oldPrimary))
	return nil
}

func (c *tsoServiceDiscovery) updateMember() error {
	resp, err := c.metacli.Get(c.ctx, []byte(c.primaryKey))
	if err != nil {
		log.Error("[tso] failed to get the keyspace serving endpoint", zap.String("primary-key", c.primaryKey), errs.ZapError(err))
		return err
	}

	if resp == nil || len(resp.Kvs) == 0 {
		log.Error("[tso] didn't find the keyspace serving endpoint", zap.String("primary-key", c.primaryKey))
		return errs.ErrClientGetServingEndpoint
	} else if resp.Count > 1 {
		return errs.ErrClientGetMultiResponse.FastGenByArgs(resp.Kvs)
	}

	value := resp.Kvs[0].Value
	primary := &tsopb.Participant{}
	if err := proto.Unmarshal(value, primary); err != nil {
		return errs.ErrClientProtoUnmarshal.Wrap(err).GenWithStackByCause()
	}
	listenUrls := primary.GetListenUrls()
	if len(listenUrls) == 0 {
		log.Error("[tso] the keyspace serving endpoint list is empty", zap.String("primary-key", c.primaryKey))
		return errs.ErrClientGetServingEndpoint
	}
	return c.switchPrimary(listenUrls)
}
