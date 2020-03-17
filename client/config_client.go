// Copyright 2019 PingCAP, Inc.
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
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// ConfigClient is a client to manage the configuration.
// It should not be used after calling Close().
type ConfigClient interface {
	GetClusterID(ctx context.Context) uint64
	Create(ctx context.Context, v *configpb.Version, component, componentID, config string) (*configpb.Status, *configpb.Version, string, error)
	GetAll(ctx context.Context) (*configpb.Status, []*configpb.LocalConfig, error)
	Get(ctx context.Context, v *configpb.Version, component, componentID string) (*configpb.Status, *configpb.Version, string, error)
	Update(ctx context.Context, v *configpb.Version, kind *configpb.ConfigKind, entries []*configpb.ConfigEntry) (*configpb.Status, *configpb.Version, error)
	Delete(ctx context.Context, v *configpb.Version, kind *configpb.ConfigKind) (*configpb.Status, error)
	// Close closes the client.
	Close()
}

type configClient struct {
	*baseClient
}

// NewConfigClient creates a PD configuration client.
func NewConfigClient(pdAddrs []string, security SecurityOption) (ConfigClient, error) {
	return NewConfigClientWithContext(context.Background(), pdAddrs, security)
}

// NewConfigClientWithContext creates a PD configuration client with the context.
func NewConfigClientWithContext(ctx context.Context, pdAddrs []string, security SecurityOption) (ConfigClient, error) {
	log.Info("[pd] create pd configuration client with endpoints", zap.Strings("pd-address", pdAddrs))
	base, err := newBaseClient(ctx, addrsToUrls(pdAddrs), security)
	if err != nil {
		return nil, err
	}
	return &configClient{base}, nil
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

func (c *configClient) GetAll(ctx context.Context) (*configpb.Status, []*configpb.LocalConfig, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("configclient.GetAll", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	start := time.Now()
	defer func() { configCmdDurationGetAll.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().GetAll(ctx, &configpb.GetAllRequest{
		Header: c.requestHeader(),
	})
	cancel()

	if err != nil {
		configCmdFailDurationGetAll.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, nil, errors.WithStack(err)
	}

	return resp.GetStatus(), resp.GetLocalConfigs(), nil
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
