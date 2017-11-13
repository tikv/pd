// Copyright 2017 PingCAP, Inc.
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

package faketikv

import (
	"context"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
	"google.golang.org/grpc"
)

// Client is a PD (Placement Driver) client.
// It should not be used after calling Close().
type Client interface {
	GetClusterID(ctx context.Context) uint64
	AllocID(ctx context.Context) (uint64, error)
	Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) error
	PutStore(ctx context.Context, store *metapb.Store) error
	StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error
	RegionHeartbeat(ctx context.Context, region *core.RegionInfo) error
	Close()
}

type regionHeartBeatRequest struct {
	start time.Time
	ctx   context.Context
	done  chan error
}

const (
	pdTimeout             = 3 * time.Second
	updateLeaderTimeout   = time.Second // Use a shorter timeout to recover faster from network isolation.
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
	urls      []string
	clusterID uint64

	connMu struct {
		sync.RWMutex
		clientConns map[string]*grpc.ClientConn
		leader      string
	}

	checkLeaderCh           chan struct{}
	reportRegionHeartbeatCh chan *core.RegionInfo
	reciveRegionHeartbeatCh chan *pdpb.RegionHeartbeatResponse

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewClient creates a PD client.
func NewClient(pdAddrs []string) (Client, <-chan *pdpb.RegionHeartbeatResponse, error) {
	log.Infof("[pd] create pd client with endpoints %v", pdAddrs)
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		urls:                    addrsToUrls(pdAddrs),
		checkLeaderCh:           make(chan struct{}, 1),
		reportRegionHeartbeatCh: make(chan *core.RegionInfo, 1),
		reciveRegionHeartbeatCh: make(chan *pdpb.RegionHeartbeatResponse, 100),
		ctx:    ctx,
		cancel: cancel,
	}
	c.connMu.clientConns = make(map[string]*grpc.ClientConn)

	if err := c.initClusterID(); err != nil {
		return nil, nil, errors.Trace(err)
	}
	if err := c.updateLeader(); err != nil {
		return nil, nil, errors.Trace(err)
	}
	log.Infof("[pd] init cluster id %v", c.clusterID)
	c.wg.Add(2)
	go c.leaderLoop()
	go c.heartbeatStreamLoop()

	return c, c.reciveRegionHeartbeatCh, nil
}

func (c *client) updateURLs(members []*pdpb.Member) {
	urls := make([]string, 0, len(members))
	for _, m := range members {
		urls = append(urls, m.GetClientUrls()...)
	}
	c.urls = urls
}

func (c *client) initClusterID() error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	for i := 0; i < maxInitClusterRetries; i++ {
		for _, u := range c.urls {
			members, err := c.getMembers(ctx, u)
			if err != nil || members.GetHeader() == nil {
				log.Errorf("[pd] failed to get cluster id: %v", err)
				continue
			}
			c.clusterID = members.GetHeader().GetClusterId()
			return nil
		}

		time.Sleep(time.Second)
	}

	return errors.Trace(errFailInitClusterID)
}

func (c *client) updateLeader() error {
	for _, u := range c.urls {
		ctx, cancel := context.WithTimeout(c.ctx, updateLeaderTimeout)
		members, err := c.getMembers(ctx, u)
		cancel()
		if err != nil || members.GetLeader() == nil || len(members.GetLeader().GetClientUrls()) == 0 {
			continue
		}
		c.updateURLs(members.GetMembers())
		if err = c.switchLeader(members.GetLeader().GetClientUrls()); err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	return errors.Errorf("failed to get leader from %v", c.urls)
}

func (c *client) getMembers(ctx context.Context, url string) (*pdpb.GetMembersResponse, error) {
	cc, err := c.getOrCreateGRPCConn(url)
	if err != nil {
		return nil, errors.Trace(err)
	}
	members, err := pdpb.NewPDClient(cc).GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return members, nil
}

func (c *client) switchLeader(addrs []string) error {
	addr := addrs[0]

	c.connMu.RLock()
	oldLeader := c.connMu.leader
	c.connMu.RUnlock()

	if addr == oldLeader {
		return nil
	}

	log.Infof("[pd] leader switches to: %v, previous: %v", addr, oldLeader)
	if _, err := c.getOrCreateGRPCConn(addr); err != nil {
		return errors.Trace(err)
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()
	c.connMu.leader = addr
	return nil
}

func (c *client) getOrCreateGRPCConn(addr string) (*grpc.ClientConn, error) {
	c.connMu.RLock()
	conn, ok := c.connMu.clientConns[addr]
	c.connMu.RUnlock()
	if ok {
		return conn, nil
	}
	cc, err := grpc.Dial(strings.TrimLeft(addr, "http://"), grpc.WithInsecure()) // TODO: Support HTTPS.
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()
	if old, ok := c.connMu.clientConns[addr]; ok {
		cc.Close()
		return old, nil
	}

	c.connMu.clientConns[addr] = cc
	return cc, nil
}

func (c *client) leaderLoop() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	for {
		select {
		case <-c.checkLeaderCh:
		case <-time.After(time.Minute):
		case <-ctx.Done():
			return
		}

		if err := c.updateLeader(); err != nil {
			log.Errorf("[pd] failed updateLeader: %v", err)
		}
	}
}

func (c *client) createHeartbeatStream() (pdpb.PD_RegionHeartbeatClient, context.Context, context.CancelFunc) {
	var (
		stream pdpb.PD_RegionHeartbeatClient
		err    error
		cancel context.CancelFunc
		ctx    context.Context
	)
	for {
		if stream == nil {
			ctx, cancel = context.WithCancel(c.ctx)
			stream, err = c.leaderClient().RegionHeartbeat(ctx)
			if err != nil {
				log.Errorf("[pd] create region heartbeat stream error: %v", err)
				c.scheduleCheckLeader()
				cancel()
				select {
				case <-time.After(time.Second):
				}
				continue
			}
		}
		break
	}
	return stream, ctx, cancel
}

func (c *client) heartbeatStreamLoop() {
	defer c.wg.Done()
	for {
		stream, ctx, cancel := c.createHeartbeatStream()
		errCh := make(chan error, 1)
		go c.reportRegionHeartbeat(ctx, stream, errCh)
		go c.reciveRegionHeartbeat(ctx, stream, errCh)
		select {
		case err := <-errCh:
			log.Infof("heartbeat stream get error: %s", err)
		}
		cancel()
	}
}

func (c *client) reciveRegionHeartbeat(ctx context.Context, stream pdpb.PD_RegionHeartbeatClient, errCh chan error) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			errCh <- err
			c.scheduleCheckLeader()
			log.Errorf("[pd] recive regionHeartbeat error: %v", err)
			return
		}
		log.Infof("recive:%v\n", resp)
		select {
		case c.reciveRegionHeartbeatCh <- resp:
		case <-ctx.Done():
			return
		}
	}
}

func (c *client) reportRegionHeartbeat(ctx context.Context, stream pdpb.PD_RegionHeartbeatClient, errCh chan error) {
	for {
		select {
		case region := <-c.reportRegionHeartbeatCh:
			//	start := time.Now()
			request := &pdpb.RegionHeartbeatRequest{
				Header:          c.requestHeader(),
				Region:          region.Region,
				Leader:          region.Leader,
				DownPeers:       region.DownPeers,
				PendingPeers:    region.PendingPeers,
				BytesWritten:    region.WrittenBytes,
				BytesRead:       region.ReadBytes,
				ApproximateSize: uint64(region.ApproximateSize),
			}
			err := stream.Send(request)
			if err != nil {
				errCh <- err
				log.Errorf("[pd] report regionHeartbeat error: %v", err)
				c.scheduleCheckLeader()
			}
		//	log.Infof("Debug: report region %+v Sepend: %v", region, time.Since(start))
		case <-ctx.Done():
			return
		}
	}
}

func (c *client) Close() {
	c.cancel()
	c.wg.Wait()

	c.connMu.Lock()
	defer c.connMu.Unlock()
	for _, cc := range c.connMu.clientConns {
		if err := cc.Close(); err != nil {
			log.Errorf("[pd] failed close grpc clientConn: %v", err)
		}
	}
}

func (c *client) leaderClient() pdpb.PDClient {
	c.connMu.RLock()
	defer c.connMu.RUnlock()

	return pdpb.NewPDClient(c.connMu.clientConns[c.connMu.leader])
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

func (c *client) AllocID(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().AllocID(ctx, &pdpb.AllocIDRequest{
		Header: c.requestHeader(),
	})
	cancel()
	if err != nil {
		c.scheduleCheckLeader()
		return 0, err
	}
	return resp.GetId(), nil
}

func (c *client) Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) error {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	_, err := c.leaderClient().Bootstrap(ctx, &pdpb.BootstrapRequest{
		Header: c.requestHeader(),
		Store:  store,
		Region: region,
	})
	cancel()
	if err != nil {
		c.scheduleCheckLeader()
		return err
	}
	return nil
}

func (c *client) PutStore(ctx context.Context, store *metapb.Store) error {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().PutStore(ctx, &pdpb.PutStoreRequest{
		Header: c.requestHeader(),
		Store:  store,
	})
	cancel()
	if err != nil {
		c.scheduleCheckLeader()
		return err
	}
	if resp.Header.GetError() != nil {
		c.scheduleCheckLeader()
		log.Info(resp.Header.GetError())
		return nil
	}
	return nil
}

func (c *client) StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().StoreHeartbeat(ctx, &pdpb.StoreHeartbeatRequest{
		Header: c.requestHeader(),
		Stats:  stats,
	})
	cancel()
	if err != nil {
		c.scheduleCheckLeader()
		return err
	}
	if resp.Header.GetError() != nil {
		c.scheduleCheckLeader()
		log.Info(resp.Header.GetError())
		return nil
	}
	return nil
}

func (c *client) RegionHeartbeat(ctx context.Context, region *core.RegionInfo) error {
	c.reportRegionHeartbeatCh <- region
	return nil
}

func (c *client) requestHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
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
