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
	"context"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/grpcutil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// ConfigClient is a client to manage the configuration.
// It should not be used after calling Close().
type ConfigClient interface {
	Create(ctx context.Context, v *configpb.Version, component, componentID, config string) (*configpb.Status, *configpb.Version, string, error)
	Get(ctx context.Context, v *configpb.Version, component, componentID string) (*configpb.Status, *configpb.Version, string, error)
	Update(ctx context.Context, v *configpb.Version, kind *configpb.ConfigKind, entries []*configpb.ConfigEntry) (*configpb.Status, *configpb.Version, error)
	Delete(ctx context.Context, v *configpb.Version, kind *configpb.ConfigKind) (*configpb.Status, error)
	// Close closes the client.
	Close()
}

type configClient struct {
	urls      []string
	clusterID uint64
	connMu    struct {
		sync.RWMutex
		clientConns map[string]*grpc.ClientConn
		leader      string
	}

	checkLeaderCh chan struct{}

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	security SecurityOption
}

// NewConfigClient creates a PD configuration client.
func NewConfigClient(pdAddrs []string, security SecurityOption) (ConfigClient, error) {
	return NewConfigClientWithContext(context.Background(), pdAddrs, security)
}

// NewConfigClientWithContext creates a PD configuration client with the context.
func NewConfigClientWithContext(ctx context.Context, pdAddrs []string, security SecurityOption) (ConfigClient, error) {
	log.Info("[pd] create pd configuration client with endpoints", zap.Strings("pd-address", pdAddrs))
	ctx1, cancel := context.WithCancel(ctx)
	c := &configClient{
		urls:          addrsToUrls(pdAddrs),
		checkLeaderCh: make(chan struct{}, 1),
		ctx:           ctx1,
		cancel:        cancel,
		security:      security,
	}
	c.connMu.clientConns = make(map[string]*grpc.ClientConn)

	if err := c.initRetry(c.initClusterID); err != nil {
		return nil, err
	}
	if err := c.initRetry(c.updateLeader); err != nil {
		return nil, err
	}
	log.Info("[pd] init cluster id", zap.Uint64("cluster-id", c.clusterID))

	c.wg.Add(1)
	go c.leaderLoop()

	return c, nil
}

func (c *configClient) updateURLs(members []*pdpb.Member) {
	urls := make([]string, 0, len(members))
	for _, m := range members {
		urls = append(urls, m.GetClientUrls()...)
	}
	c.urls = urls
}

func (c *configClient) initRetry(f func() error) error {
	var err error
	for i := 0; i < maxInitClusterRetries; i++ {
		if err = f(); err == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return errors.WithStack(err)
}

func (c *configClient) initClusterID() error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	for _, u := range c.urls {
		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, pdTimeout)
		members, err := c.getMembers(timeoutCtx, u)
		timeoutCancel()
		if err != nil || members.GetHeader() == nil {
			log.Warn("[pd] failed to get cluster id", zap.String("url", u), zap.Error(err))
			continue
		}
		c.clusterID = members.GetHeader().GetClusterId()
		return nil
	}
	return errors.WithStack(errFailInitClusterID)
}

func (c *configClient) updateLeader() error {
	for _, u := range c.urls {
		ctx, cancel := context.WithTimeout(c.ctx, updateLeaderTimeout)
		members, err := c.getMembers(ctx, u)
		cancel()
		if err != nil || members.GetLeader() == nil || len(members.GetLeader().GetClientUrls()) == 0 {
			select {
			case <-c.ctx.Done():
				return errors.WithStack(err)
			default:
				continue
			}
		}
		c.updateURLs(members.GetMembers())
		return c.switchLeader(members.GetLeader().GetClientUrls())
	}
	return errors.Errorf("failed to get leader from %v", c.urls)
}

func (c *configClient) getMembers(ctx context.Context, url string) (*pdpb.GetMembersResponse, error) {
	cc, err := c.getOrCreateGRPCConn(url)
	if err != nil {
		return nil, err
	}
	members, err := pdpb.NewPDClient(cc).GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return members, nil
}

func (c *configClient) switchLeader(addrs []string) error {
	// FIXME: How to safely compare leader urls? For now, only allows one client url.
	addr := addrs[0]

	c.connMu.RLock()
	oldLeader := c.connMu.leader
	c.connMu.RUnlock()

	if addr == oldLeader {
		return nil
	}

	log.Info("[pd] switch leader", zap.String("new-leader", addr), zap.String("old-leader", oldLeader))
	if _, err := c.getOrCreateGRPCConn(addr); err != nil {
		return err
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()
	c.connMu.leader = addr
	return nil
}

func (c *configClient) getOrCreateGRPCConn(addr string) (*grpc.ClientConn, error) {
	c.connMu.RLock()
	conn, ok := c.connMu.clientConns[addr]
	c.connMu.RUnlock()
	if ok {
		return conn, nil
	}

	cc, err := grpcutil.GetClientConn(addr, c.security.CAPath, c.security.CertPath, c.security.KeyPath)
	if err != nil {
		return nil, errors.WithStack(err)
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

func (c *configClient) leaderLoop() {
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
			log.Error("[pd] failed updateLeader", zap.Error(err))
		}
	}
}

func (c *configClient) Close() {
	c.cancel()
	c.wg.Wait()

	c.connMu.Lock()
	defer c.connMu.Unlock()
	for _, cc := range c.connMu.clientConns {
		if err := cc.Close(); err != nil {
			log.Error("[pd] failed close grpc clientConn", zap.Error(err))
		}
	}
}

// leaderClient gets the client of current PD leader.
func (c *configClient) leaderClient() configpb.ConfigClient {
	c.connMu.RLock()
	defer c.connMu.RUnlock()

	return configpb.NewConfigClient(c.connMu.clientConns[c.connMu.leader])
}

func (c *configClient) ScheduleCheckLeader() {
	select {
	case c.checkLeaderCh <- struct{}{}:
	default:
	}
}

func (c *configClient) GetClusterID(context.Context) uint64 {
	return c.clusterID
}

// For testing use.
func (c *configClient) GetLeaderAddr() string {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.connMu.leader
}

// For testing use. It should only be called when the client is closed.
func (c *configClient) GetURLs() []string {
	return c.urls
}

func (c *configClient) Create(ctx context.Context, v *configpb.Version, component, componentID, config string) (*configpb.Status, *configpb.Version, string, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("configclient.Create", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	start := time.Now()
	defer func() { configCmdDurationCreate.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().Create(ctx, &configpb.CreateRequest{
		Header:      c.requestHeader(),
		Version:     v,
		Component:   component,
		ComponentId: componentID,
		Config:      config,
	})
	cancel()

	if err != nil {
		configCmdFailDurationCreate.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, nil, "", errors.WithStack(err)
	}

	return resp.GetStatus(), resp.GetVersion(), resp.GetConfig(), nil
}

func (c *configClient) Get(ctx context.Context, v *configpb.Version, component, componentID string) (*configpb.Status, *configpb.Version, string, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("configclient.Get", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	start := time.Now()
	defer func() { configCmdDurationGet.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().Get(ctx, &configpb.GetRequest{
		Header:      c.requestHeader(),
		Version:     v,
		Component:   component,
		ComponentId: componentID,
	})
	cancel()

	if err != nil {
		configCmdFailDurationGet.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, nil, "", errors.WithStack(err)
	}

	return resp.GetStatus(), resp.GetVersion(), resp.GetConfig(), nil
}

func (c *configClient) Update(ctx context.Context, v *configpb.Version, kind *configpb.ConfigKind, entries []*configpb.ConfigEntry) (*configpb.Status, *configpb.Version, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("configclient.Update", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	start := time.Now()
	defer func() { configCmdDurationUpdate.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().Update(ctx, &configpb.UpdateRequest{
		Header:  c.requestHeader(),
		Version: v,
		Kind:    kind,
		Entries: entries,
	})
	cancel()

	if err != nil {
		configCmdFailDurationUpdate.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, nil, errors.WithStack(err)
	}

	return resp.GetStatus(), resp.GetVersion(), nil
}

func (c *configClient) Delete(ctx context.Context, v *configpb.Version, kind *configpb.ConfigKind) (*configpb.Status, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("configclient.Delete", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	start := time.Now()
	defer func() { configCmdDurationDelete.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().Update(ctx, &configpb.UpdateRequest{
		Header:  c.requestHeader(),
		Version: v,
		Kind:    kind,
	})
	cancel()

	if err != nil {
		configCmdFailDurationDelete.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}

	return resp.GetStatus(), nil
}

func (c *configClient) requestHeader() *configpb.Header {
	return &configpb.Header{
		ClusterId: c.clusterID,
	}
}
