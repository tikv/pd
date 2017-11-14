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

const (
	pdTimeout             = time.Second
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
	url        string
	clusterID  uint64
	clientConn *grpc.ClientConn
	pdClient   pdpb.PDClient

	reportRegionHeartbeatCh chan *core.RegionInfo
	reciveRegionHeartbeatCh chan *pdpb.RegionHeartbeatResponse

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewClient creates a PD client.
func NewClient(pdAddr string) (Client, <-chan *pdpb.RegionHeartbeatResponse, error) {
	log.Infof("[pd] create pd client with endpoints %v", pdAddr)
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		url: addrsToUrls(pdAddr),
		reportRegionHeartbeatCh: make(chan *core.RegionInfo, 1),
		reciveRegionHeartbeatCh: make(chan *pdpb.RegionHeartbeatResponse, 1),
		ctx:    ctx,
		cancel: cancel,
	}
	cc, err := c.createGRPCConn()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	c.pdClient = pdpb.NewPDClient(cc)
	c.clientConn = cc
	if err := c.initClusterID(); err != nil {
		return nil, nil, errors.Trace(err)
	}
	log.Infof("[pd] init cluster id %v", c.clusterID)
	c.wg.Add(1)
	go c.heartbeatStreamLoop()

	return c, c.reciveRegionHeartbeatCh, nil
}

func (c *client) initClusterID() error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	for i := 0; i < maxInitClusterRetries; i++ {
		members, err := c.getMembers(ctx)
		if err != nil || members.GetHeader() == nil {
			log.Errorf("[pd] failed to get cluster id: %v", err)
			continue
		}
		c.clusterID = members.GetHeader().GetClusterId()
		return nil
	}

	return errors.Trace(errFailInitClusterID)
}

func (c *client) getMembers(ctx context.Context) (*pdpb.GetMembersResponse, error) {
	members, err := c.pdClient.GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return members, nil
}

func (c *client) createGRPCConn() (*grpc.ClientConn, error) {
	cc, err := grpc.Dial(strings.TrimLeft(c.url, "http://"), grpc.WithInsecure())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return cc, nil
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
			stream, err = c.pdClient.RegionHeartbeat(ctx)
			if err != nil {
				log.Errorf("[pd] create region heartbeat stream error: %v", err)
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
	var i int
	for {
		i++
		stream, ctx, cancel := c.createHeartbeatStream()
		errCh := make(chan error, 1)
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go c.reportRegionHeartbeat(ctx, stream, errCh, wg)
		go c.reciveRegionHeartbeat(ctx, stream, errCh, wg)
		select {
		case err := <-errCh:
			log.Infof("[pd] heartbeat stream get error: %s", err)
			cancel()
		case <-c.ctx.Done():
			return
		}
		wg.Wait()
	}
}

func (c *client) reciveRegionHeartbeat(ctx context.Context, stream pdpb.PD_RegionHeartbeatClient, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		resp, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		select {
		case c.reciveRegionHeartbeatCh <- resp:
		case <-ctx.Done():
			return
		}
	}
}

func (c *client) reportRegionHeartbeat(ctx context.Context, stream pdpb.PD_RegionHeartbeatClient, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case region := <-c.reportRegionHeartbeatCh:
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
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *client) Close() {
	c.cancel()
	c.wg.Wait()

	if err := c.clientConn.Close(); err != nil {
		log.Errorf("[pd] failed close grpc clientConn: %v", err)
	}
}

func (c *client) GetClusterID(context.Context) uint64 {
	return c.clusterID
}

func (c *client) AllocID(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient.AllocID(ctx, &pdpb.AllocIDRequest{
		Header: c.requestHeader(),
	})
	cancel()
	if err != nil {
		return 0, err
	}
	return resp.GetId(), nil
}

func (c *client) Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) error {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	_, err := c.pdClient.Bootstrap(ctx, &pdpb.BootstrapRequest{
		Header: c.requestHeader(),
		Store:  store,
		Region: region,
	})
	cancel()
	if err != nil {
		return err
	}
	return nil
}

func (c *client) PutStore(ctx context.Context, store *metapb.Store) error {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient.PutStore(ctx, &pdpb.PutStoreRequest{
		Header: c.requestHeader(),
		Store:  store,
	})
	cancel()
	if err != nil {
		return err
	}
	if resp.Header.GetError() != nil {
		log.Info(resp.Header.GetError())
		return nil
	}
	return nil
}

func (c *client) StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient.StoreHeartbeat(ctx, &pdpb.StoreHeartbeatRequest{
		Header: c.requestHeader(),
		Stats:  stats,
	})
	cancel()
	if err != nil {
		return err
	}
	if resp.Header.GetError() != nil {
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

func addrsToUrls(addr string) string {
	// Add default schema "http://" to addrs.
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	return addr
}
