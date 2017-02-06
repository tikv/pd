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
	GetClusterID() uint64
	// GetTS gets a timestamp from PD.
	GetTS() (int64, int64, error)
	// GetRegion gets a region and its leader Peer from PD by key.
	// The region may expire after split. Caller is responsible for caching and
	// taking care of region change.
	// Also it may return nil if PD finds no Region for the key temporarily,
	// client should retry later.
	GetRegion(key []byte) (*metapb.Region, *metapb.Peer, error)
	// GetStore gets a store from PD by store id.
	// The store may expire later. Caller is responsible for caching and taking care
	// of store change.
	GetStore(storeID uint64) (*metapb.Store, error)
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

type client struct {
	urls        []string
	clusterID   uint64
	tsoRequests chan *tsoRequest

	mu            sync.RWMutex
	clients       map[string]pdpb2.PDClient
	leader        string
	checkLeaderCh chan struct{}

	wg   sync.WaitGroup
	quit chan struct{}
}

// NewClient creates a PD client.
func NewClient(pdAddrs []string) (Client, error) {
	log.Infof("[pd] create pd client with endpoints %v", pdAddrs)
	c := &client{
		urls:          addrsToUrls(pdAddrs),
		tsoRequests:   make(chan *tsoRequest, maxMergeTSORequests),
		clients:       make(map[string]pdpb2.PDClient),
		checkLeaderCh: make(chan struct{}, 1),
		quit:          make(chan struct{}),
	}

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

	return errors.New("failed to get cluster id")
}

func (c *client) updateLeader() error {
	for _, u := range c.urls {
		client, err := apiutil.NewClient(u, pdTimeout)
		if err != nil {
			continue
		}
		leader, err := client.GetLeader()
		if err != nil {
			continue
		}
		c.mu.RLock()
		changed := c.leader != leader.GetAddr()
		c.mu.RUnlock()
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.clients[addr]; !ok {
		cc, err := grpc.Dial(addr, grpc.WithDialer(func(addr string, d time.Duration) (net.Conn, error) {
			u, err := url.Parse(addr)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return net.Dial(u.Scheme, u.Host)
		}), grpc.WithInsecure())
		if err != nil {
			return errors.Trace(err)
		}
		c.clients[addr] = pdpb2.NewPDClient(cc)
	}
	c.leader = addr
	return nil
}

func (c *client) leaderLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.checkLeaderCh:
		case <-time.After(time.Minute):
		case <-c.quit:
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
		case firstReq := <-c.tsoRequests:
			start := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), pdTimeout)
			defer cancel()

			pendingCount := len(c.tsoRequests)
			resp, err := c.leaderClient().Tso(ctx, &pdpb2.TsoRequest{
				Header: &pdpb2.RequestHeader{ClusterId: c.clusterID},
				Count:  uint32(pendingCount + 1),
			})
			requestDuration.WithLabelValues("tso").Observe(time.Since(start).Seconds())
			if err == nil && resp.GetCount() != uint32(pendingCount+1) {
				err = errors.New("[pd] tso length in rpc response is incorrect")
			}
			if err != nil {
				firstReq.done <- errors.Trace(err)
				for i := 0; i < pendingCount; i++ {
					req := <-c.tsoRequests
					req.done <- errors.Trace(err)
				}
			}
			tsHigh := resp.GetTimestamp()
			firstReq.physical = tsHigh.GetPhysical()
			firstReq.logical = tsHigh.GetLogical() - int64(pendingCount)
			firstReq.done <- nil
			for i := 1; i <= pendingCount; i++ {
				req := <-c.tsoRequests
				req.physical = tsHigh.GetPhysical()
				req.logical = tsHigh.GetLogical() - int64(resp.GetCount()) + int64(i)
				req.done <- nil
			}
		case <-c.quit:
			return
		}
	}
}

func (c *client) Close() {
	close(c.quit)
	c.wg.Wait()

	n := len(c.tsoRequests)
	for i := 0; i < n; i++ {
		req := <-c.tsoRequests
		req.done <- errors.New("[pd] closing")
	}
}

func (c *client) leaderClient() pdpb2.PDClient {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.clients[c.leader]
}

func (c *client) GetClusterID() uint64 {
	return c.clusterID
}

func (c *client) checkLeader() {
	select {
	case c.checkLeaderCh <- struct{}{}:
	default:
	}
}

func (c *client) GetTS() (int64, int64, error) {
	cmdCounter.WithLabelValues("tso").Inc()

	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("tso").Observe(time.Since(start).Seconds()) }()

	req := &tsoRequest{
		done: make(chan error, 1),
	}
	c.tsoRequests <- req
	err := <-req.done

	if err != nil {
		cmdFailedCounter.WithLabelValues("tso").Inc()
		c.checkLeader()
		return 0, 0, errors.Trace(err)
	}

	return req.physical, req.logical, err
}

func (c *client) GetRegion(key []byte) (*metapb.Region, *metapb.Peer, error) {
	cmdCounter.WithLabelValues("get_region").Inc()

	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("get_region").Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(context.Background(), pdTimeout)
	defer cancel()

	resp, err := c.leaderClient().GetRegion(ctx, &pdpb2.GetRegionRequest{RegionKey: key})
	requestDuration.WithLabelValues("get_region").Observe(time.Since(start).Seconds())

	if err != nil {
		cmdFailedCounter.WithLabelValues("get_region").Inc()
		c.checkLeader()
		return nil, nil, errors.Trace(err)
	}
	return resp.GetRegion(), resp.GetLeader(), nil
}

func (c *client) GetStore(storeID uint64) (*metapb.Store, error) {
	cmdCounter.WithLabelValues("get_store").Inc()

	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("get_store").Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(context.Background(), pdTimeout)
	defer cancel()

	resp, err := c.leaderClient().GetStore(ctx, &pdpb2.GetStoreRequest{StoreId: storeID})
	requestDuration.WithLabelValues("get_store").Observe(time.Since(start).Seconds())

	if err != nil {
		cmdFailedCounter.WithLabelValues("get_store").Inc()
		c.checkLeader()
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
