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
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/grpcutil"
	"github.com/tikv/pd/client/tlsutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	// tsoKeyspaceGroupPrimaryElectionPrefix defines the key prefix for keyspace group primary election.
	// The entire key is in the format of "/pd/<cluster-id>/microservice/tso/keyspace-group-XXXXX/primary" in which
	// XXXXX is 5 digits integer with leading zeros. For now we use 0 as the default cluster id.
	tsoKeyspaceGroupPrimaryElectionPrefix = "/pd/0/microservice/tso/keyspace-group-"
)

var _ ServiceDiscovery = (*tsoMcsDiscovery)(nil)
var _ tsoAllocatorEventSource = (*tsoMcsDiscovery)(nil)

// tsoMcsDiscovery is the service discovery client of TSO microservice which is primary/secondary configured
type tsoMcsDiscovery struct {
	keyspaceID uint32
	// primary key is the etcd path used for discoverying the serving endpoint of this keyspace
	primaryKey string
	urls       atomic.Value // Store as []string
	// TSO Primary URL
	primary atomic.Value // Store as string
	// TSO Secondary URLs
	secondaries atomic.Value // Store as []string
	metacli     MetaStorageClient

	// addr -> a gRPC connection
	clientConns sync.Map // Store as map[string]*grpc.ClientConn

	// primarySwitchedCallbacks will be called after the primary swichted
	primarySwitchedCallbacks []func()
	// membersChangedCallbacks will be called after there is any membership
	// change in the primary and followers
	membersChangedCallbacks []func()
	// tsoAllocPrimariesUpdatedCallback will be called when the global/local
	// tso allocator primary list is updated. The input is a map {DC Localtion -> Leader Addr}
	tsoAllocPrimariesUpdatedCallback []tsoAllocServingAddrUpdatedFunc

	checkMembershipCh chan struct{}

	wg     *sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	tlsCfg *tlsutil.TLSConfig

	// Client option.
	option *option
}

// newTSOMcsDiscovery returns a new BaseClient of a TSO microservice.
func newTSOMcsDiscovery(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, metacli MetaStorageClient,
	keyspaceID uint32, urls []string, tlsCfg *tlsutil.TLSConfig, option *option) ServiceDiscovery {
	bc := &tsoMcsDiscovery{
		ctx:               ctx,
		cancel:            cancel,
		wg:                wg,
		metacli:           metacli,
		keyspaceID:        keyspaceID,
		primaryKey:        path.Join(tsoKeyspaceGroupPrimaryElectionPrefix+fmt.Sprintf("%05d", 0), "primary"),
		tlsCfg:            tlsCfg,
		option:            option,
		checkMembershipCh: make(chan struct{}, 1),
	}
	bc.urls.Store(urls)

	return bc
}

// Init initialize the concrete client underlying
func (c *tsoMcsDiscovery) Init() error {
	if err := c.initRetry(c.updateMember); err != nil {
		c.cancel()
		return err
	}
	c.wg.Add(1)
	go c.startCheckMemberLoop()
	return nil
}

func (c *tsoMcsDiscovery) initRetry(f func() error) error {
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

func (c *tsoMcsDiscovery) startCheckMemberLoop() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	for {
		select {
		case <-c.checkMembershipCh:
		case <-time.After(memberUpdateInterval):
		case <-ctx.Done():
			return
		}
		if err := c.updateMember(); err != nil {
			log.Error("[pd(tso)] failed to update member", errs.ZapError(err))
		}
	}
}

// Close releases all resources
func (c *tsoMcsDiscovery) Close() {
	c.clientConns.Range(func(key, cc interface{}) bool {
		if err := cc.(*grpc.ClientConn).Close(); err != nil {
			log.Error("[pd(tso)] failed to close gRPC clientConn", errs.ZapError(errs.ErrCloseGRPCConn, err))
		}
		c.clientConns.Delete(key)
		return true
	})
}

// GetClusterID returns the ID of the cluster
func (c *tsoMcsDiscovery) GetClusterID(context.Context) uint64 {
	return 0
}

// GetURLs returns the URLs of the servers.
// For testing use. It should only be called when the client is closed.
func (c *tsoMcsDiscovery) GetURLs() []string {
	return c.urls.Load().([]string)
}

// GetServingAddr returns the grpc client connection of the serving endpoint
// which is the primary in a primary/secondy configured cluster.
func (c *tsoMcsDiscovery) GetServingEndpointClientConn() *grpc.ClientConn {
	if cc, ok := c.clientConns.Load(c.getPrimaryAddr()); ok {
		return cc.(*grpc.ClientConn)
	}
	return nil
}

// GetClientConns returns the mapping {addr -> a gRPC connectio}
func (c *tsoMcsDiscovery) GetClientConns() *sync.Map {
	return &c.clientConns
}

// GetServingAddr returns the serving endpoint which is the primary in a
// primary/secondy configured cluster.
func (c *tsoMcsDiscovery) GetServingAddr() string {
	return c.getPrimaryAddr()
}

// GetBackupAddrs gets the addresses of the current reachable and healthy
// backup service endpoints randomly. Backup service endpoints are secondaries in
// a primary/secondary configured cluster.
func (c *tsoMcsDiscovery) GetBackupAddrs() []string {
	return c.getSecondaryAddrs()
}

// GetOrCreateGRPCConn returns the corresponding grpc client connection of the given addr.
func (c *tsoMcsDiscovery) GetOrCreateGRPCConn(addr string) (*grpc.ClientConn, error) {
	return grpcutil.GetOrCreateGRPCConn(c.ctx, &c.clientConns, addr, c.tlsCfg, c.option.gRPCDialOptions...)
}

// ScheduleCheckMemberChanged is used to trigger a check to see if there is any change in ervice endpoints.
func (c *tsoMcsDiscovery) ScheduleCheckMemberChanged() {
	select {
	case c.checkMembershipCh <- struct{}{}:
	default:
	}
}

// Immediately checkif there is any membership change among the primary/secondaries in
// a primary/secondy configured cluster.
func (c *tsoMcsDiscovery) CheckMemberChanged() error {
	return c.updateMember()
}

// AddServingAddrSwitchedCallback adds callbacks which will be called when the primary in
// a primary/secondary configured cluster is switched.
func (c *tsoMcsDiscovery) AddServingAddrSwitchedCallback(callbacks ...func()) {
	c.primarySwitchedCallbacks = append(c.primarySwitchedCallbacks, callbacks...)
}

// AddServiceAddrsSwitchedCallback adds callbacks which will be called when any primary/secondary
// in a primary/secondary configured cluster is changed.
func (c *tsoMcsDiscovery) AddServiceAddrsSwitchedCallback(callbacks ...func()) {
	c.membersChangedCallbacks = append(c.membersChangedCallbacks, callbacks...)
}

// AddTSOAllocServingAddrsUpdatedCallback adds callbacks which will be called
// when the global/local tso allocator leader list is updated.
func (c *tsoMcsDiscovery) AddTSOAllocServingAddrsUpdatedCallback(callbacks ...tsoAllocServingAddrUpdatedFunc) {
	c.tsoAllocPrimariesUpdatedCallback = append(c.tsoAllocPrimariesUpdatedCallback, callbacks...)
}

// getPrimaryAddr returns the primary address.
func (c *tsoMcsDiscovery) getPrimaryAddr() string {
	primaryAddr := c.primary.Load()
	if primaryAddr == nil {
		return ""
	}
	return primaryAddr.(string)
}

// getSecondaryAddrs returns the secondary addresses.
func (c *tsoMcsDiscovery) getSecondaryAddrs() []string {
	secondaryAddrs := c.secondaries.Load()
	if secondaryAddrs == nil {
		return []string{}
	}
	return secondaryAddrs.([]string)
}

func (c *tsoMcsDiscovery) switchPrimary(addrs []string) error {
	// FIXME: How to safely compare primary urls? For now, only allows one client url.
	addr := addrs[0]
	oldPrimary := c.getPrimaryAddr()
	if addr == oldPrimary {
		return nil
	}

	if _, err := c.GetOrCreateGRPCConn(addr); err != nil {
		log.Warn("[pd(tso)] failed to connect primary", zap.String("primary", addr), errs.ZapError(err))
		return err
	}
	// Set PD primary and Global TSO Allocator (which is also the PD primary)
	c.primary.Store(addr)
	c.switchTSOAllocatorPrimary(globalDCLocation, addr)
	// Run callbacks
	for _, cb := range c.primarySwitchedCallbacks {
		cb()
	}
	log.Info("[pd(tso)] switch primary", zap.String("new-primary", addr), zap.String("old-primary", oldPrimary))
	return nil
}

func (c *tsoMcsDiscovery) switchTSOAllocatorPrimary(dcLocation string, addr string) error {
	allocMap := map[string]string{dcLocation: addr}

	// Run callbacks to refelect any possible change in the global/local tso allocator.
	for _, cb := range c.tsoAllocPrimariesUpdatedCallback {
		if err := cb(allocMap); err != nil {
			return err
		}
	}

	return nil
}

func (c *tsoMcsDiscovery) updateMember() error {
	resp, err := c.metacli.Get(c.ctx, []byte(c.primaryKey))
	if err != nil {
		log.Error("[pd(tso)] failed to get the keyspace serving endpoint", errs.ZapError(err))
		return err
	}

	if resp == nil || len(resp.Kvs) == 0 {
		log.Error("[pd(tso)] didn't find the keyspace serving endpoint")
		return errs.ErrClientGetLeader
	} else if resp.Count > 1 {
		return errs.ErrClientGetMultiResponse.FastGenByArgs(resp.Kvs)
	}

	value := resp.Kvs[0].Value
	member := &pdpb.Member{}
	if err := proto.Unmarshal(value, member); err != nil {
		return errs.ErrClientProtoUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return c.switchPrimary(addrsToUrls([]string{member.Name}))
}
