// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package pd

import (
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb2"
	"github.com/pingcap/pd/pkg/apiutil"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Client is a PD (Placement Driver) client.
// It should not be used after calling Close().
type Client interface {
	// GetClusterID gets the cluster ID from PD.
	GetClusterID(ctx context.Context) uint64
	// GetTS gets a timestamp from PD.
	GetTS(ctx context.Context) (int64, int64, error)
	// GetRegion gets a region and its leader Peer from PD by key.
	// The region may expire after split. Caller is responsible for caching and
	// taking care of region change.
	// Also it may return nil if PD finds no Region for the key temporarily,
	// client should retry later.
	GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error)
	// GetStore gets a store from PD by store id.
	// The store may expire later. Caller is responsible for caching and taking care
	// of store change.
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	// Close closes the client.
	Close()
}

type tsoRequest struct {
	done     chan error
	physical int64
	logical  int64
}

const (
	pdTimeout             = 3 * time.Second
	maxMergeTSORequests   = 10000
	maxInitClusterRetries = 100
)

var (
	// errFailInitClusterID is returned when failed to load clusterID from all supplied PD addresses.
	errFailInitClusterID = errors.New("[pd] failed to get cluster id")
	// errClosing is returned when request is canceled when client is closing.
	errClosing = errors.New("[pd] closing")
	// errTSOLength is returned when the number of response timestamps is inconsistent with request.
	errTSOLength = errors.New("[pd] tso length in rpc response is incorrect")
)

type client struct {
	urls        []string
	clusterID   uint64
	tsoRequests chan *tsoRequest

	connMu struct {
		sync.RWMutex
		clientConns map[string]*grpc.ClientConn
		leader      string
	}

	checkLeaderCh chan struct{}

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewClient creates a PD client.
func NewClient(pdAddrs []string) (Client, error) {
	log.Infof("[pd] create pd client with endpoints %v", pdAddrs)
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		urls:          addrsToUrls(pdAddrs),
		tsoRequests:   make(chan *tsoRequest, maxMergeTSORequests),
		checkLeaderCh: make(chan struct{}, 1),
		ctx:           ctx,
		cancel:        cancel,
	}
	c.connMu.clientConns = make(map[string]*grpc.ClientConn)

	if err := c.initClusterID(); err != nil {
		return nil, errors.Trace(err)
	}
	if err := c.updateLeader(); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("[pd] init cluster id %v", c.clusterID)

	c.wg.Add(2)
	go c.tsLoop()
	go c.leaderLoop()

	// TODO: Update addrs from server continuously by using GetMember.

	return c, nil
}

func (c *client) initClusterID() error {
	for i := 0; i < maxInitClusterRetries; i++ {
		for _, u := range c.urls {
			// TODO: Use gRPC instead.
			client, err := apiutil.NewClient(u, pdTimeout)
			if err != nil {
				log.Errorf("[pd] failed to get cluster id: %v", err)
				continue
			}
			clusterID, err := client.GetClusterID()
			if err != nil {
				log.Errorf("[pd] failed to get cluster id: %v", err)
				continue
			}
			c.clusterID = clusterID
			return nil
		}

		time.Sleep(time.Second)
	}

	return errors.Trace(errFailInitClusterID)
}

func (c *client) updateLeader() error {
	for _, u := range c.urls {
		// TODO: Use gRPC instead.
		client, err := apiutil.NewClient(u, pdTimeout)
		if err != nil {
			continue
		}
		leader, err := client.GetLeader()
		if err != nil {
			continue
		}
		c.connMu.RLock()
		changed := c.connMu.leader != leader.GetAddr()
		c.connMu.RUnlock()
		if changed {
			if err = c.switchLeader(leader.GetAddr()); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}
	return errors.Errorf("failed to get leader from %v", c.urls)
}

func (c *client) switchLeader(addr string) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	log.Infof("[pd] leader switches to: %v, previous: %v", addr, c.connMu.leader)
	if _, ok := c.connMu.clientConns[addr]; !ok {
		cc, err := grpc.Dial(addr, grpc.WithDialer(func(addr string, d time.Duration) (net.Conn, error) {
			u, err := url.Parse(addr)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return net.DialTimeout(u.Scheme, u.Host, d)
		}), grpc.WithInsecure()) // TODO: Support HTTPS.
		if err != nil {
			return errors.Trace(err)
		}
		c.connMu.clientConns[addr] = cc
	}
	c.connMu.leader = addr
	return nil
}

func (c *client) leaderLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.checkLeaderCh:
		case <-time.After(time.Minute):
		case <-c.ctx.Done():
			return
		}

		if err := c.updateLeader(); err != nil {
			log.Errorf("[pd] failed updateLeader: %v", err)
		}
	}
}

func (c *client) tsLoop() {
	defer c.wg.Done()

	for {
		select {
		case first := <-c.tsoRequests:
			c.processTSORequests(first)
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *client) processTSORequests(first *tsoRequest) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, pdTimeout)

	pendingCount := len(c.tsoRequests)
	resp, err := c.leaderClient().Tso(ctx, &pdpb2.TsoRequest{
		Header: c.requestHeader(),
		Count:  uint32(pendingCount + 1),
	})
	cancel()
	requestDuration.WithLabelValues("tso").Observe(time.Since(start).Seconds())
	if err == nil && resp.GetCount() != uint32(pendingCount+1) {
		err = errTSOLength
	}
	if err != nil {
		c.finishTSORequest(first, 0, 0, errors.Trace(err))
		for i := 0; i < pendingCount; i++ {
			c.finishTSORequest(<-c.tsoRequests, 0, 0, errors.Trace(err))
		}
		return
	}

	physical, logical := resp.GetTimestamp().GetPhysical(), resp.GetTimestamp().GetLogical()
	// Server returns the highest ts.
	logical -= int64(resp.GetCount() - 1)
	c.finishTSORequest(first, physical, logical, nil)
	for i := 0; i < pendingCount; i++ {
		logical++
		c.finishTSORequest(<-c.tsoRequests, physical, logical, nil)
	}
}

func (c *client) finishTSORequest(req *tsoRequest, physical, logical int64, err error) {
	req.physical, req.logical = physical, logical
	req.done <- err
}

func (c *client) Close() {
	c.cancel()
	c.wg.Wait()

	n := len(c.tsoRequests)
	for i := 0; i < n; i++ {
		req := <-c.tsoRequests
		req.done <- errors.Trace(errClosing)
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()
	for _, cc := range c.connMu.clientConns {
		if err := cc.Close(); err != nil {
			log.Errorf("[pd] failed close grpc clientConn: %v", err)
		}
	}
}

func (c *client) leaderClient() pdpb2.PDClient {
	c.connMu.RLock()
	defer c.connMu.RUnlock()

	return pdpb2.NewPDClient(c.connMu.clientConns[c.connMu.leader])
}

func (c *client) scheduleCheckLeader() {
	select {
	case c.checkLeaderCh <- struct{}{}:
	default:
	}
}

func (c *client) GetClusterID(context.Context) uint64 {
	return c.clusterID
}

func (c *client) GetTS(ctx context.Context) (int64, int64, error) {
	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("tso").Observe(time.Since(start).Seconds()) }()

	req := &tsoRequest{
		done: make(chan error, 1),
	}
	c.tsoRequests <- req

	select {
	case err := <-req.done:
		if err != nil {
			cmdFailedDuration.WithLabelValues("tso").Observe(time.Since(start).Seconds())
			c.scheduleCheckLeader()
			return 0, 0, errors.Trace(err)
		}
		return req.physical, req.logical, err
	case <-ctx.Done():
		return 0, 0, errors.Trace(ctx.Err())
	}
}

func (c *client) GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("get_region").Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().GetRegion(ctx, &pdpb2.GetRegionRequest{
		Header:    c.requestHeader(),
		RegionKey: key,
	})
	requestDuration.WithLabelValues("get_region").Observe(time.Since(start).Seconds())
	cancel()

	if err != nil {
		cmdFailedDuration.WithLabelValues("get_region").Observe(time.Since(start).Seconds())
		c.scheduleCheckLeader()
		return nil, nil, errors.Trace(err)
	}
	return resp.GetRegion(), resp.GetLeader(), nil
}

func (c *client) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("get_store").Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().GetStore(ctx, &pdpb2.GetStoreRequest{
		Header:  c.requestHeader(),
		StoreId: storeID,
	})
	requestDuration.WithLabelValues("get_store").Observe(time.Since(start).Seconds())
	cancel()

	if err != nil {
		cmdFailedDuration.WithLabelValues("get_store").Observe(time.Since(start).Seconds())
		c.scheduleCheckLeader()
		return nil, errors.Trace(err)
	}
	store := resp.GetStore()
	if store == nil {
		return nil, errors.New("[pd] store field in rpc response not set")
	}
	if store.GetState() == metapb.StoreState_Tombstone {
		return nil, nil
	}
	return store, nil
}

func (c *client) requestHeader() *pdpb2.RequestHeader {
	return &pdpb2.RequestHeader{
		ClusterId: c.clusterID,
	}
}

func addrsToUrls(addrs []string) []string {
	// Add default schema "http://" to addrs.
	urls := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if strings.Contains(addr, "://") {
			urls = append(urls, addr)
		} else {
			urls = append(urls, "http://"+addr)
		}
	}
	return urls
}
