// Copyright 2024 TiKV Project Authors.
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

package cases

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	pdHttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

var base = int64(time.Second) / int64(time.Microsecond)

type Coordinator struct {
	ctx context.Context

	httpClients []pdHttp.Client
	gRPCClients []pd.Client

	http map[string]*HTTPController
	grpc map[string]*GRPCController

	mu sync.Mutex
}

func NewCoordinator(ctx context.Context, httpClients []pdHttp.Client, gRPCClients []pd.Client) *Coordinator {
	return &Coordinator{
		ctx:         ctx,
		httpClients: httpClients,
		gRPCClients: gRPCClients,
		http:        make(map[string]*HTTPController),
		grpc:        make(map[string]*GRPCController),
	}
}

func (c *Coordinator) GetHTTPCase(name string) (*Config, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if controller, ok := c.http[name]; ok {
		return controller.GetConfig(), nil
	}
	return nil, errors.Errorf("case %v does not exist.", name)
}

func (c *Coordinator) GetGRPCCase(name string) (*Config, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if controller, ok := c.grpc[name]; ok {
		return controller.GetConfig(), nil
	}
	return nil, errors.Errorf("case %v does not exist.", name)
}

func (c *Coordinator) GetAllHTTPCases() map[string]*Config {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := make(map[string]*Config)
	for name, c := range c.http {
		ret[name] = c.GetConfig()
	}
	return ret
}

func (c *Coordinator) GetAllGRPCCases() map[string]*Config {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := make(map[string]*Config)
	for name, c := range c.grpc {
		ret[name] = c.GetConfig()
	}
	return ret
}

func (c *Coordinator) SetHTTPCase(name string, cfg *Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fn, ok := HTTPCaseFnMap[name]; ok {
		var controller *HTTPController
		if controller, ok = c.http[name]; !ok {
			controller = NewHTTPController(c.ctx, c.httpClients, fn)
			c.http[name] = controller
		}
		controller.Stop()
		controller.SetQPS(cfg.QPS)
		if cfg.Burst > 0 {
			controller.SetBurst(cfg.Burst)
		}
		controller.Run()
	} else {
		return errors.Errorf("HTTP case %s not implemented", name)
	}
	return nil
}

func (c *Coordinator) SetGRPCCase(name string, cfg *Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fn, ok := GRPCCaseFnMap[name]; ok {
		var controller *GRPCController
		if controller, ok = c.grpc[name]; !ok {
			controller = NewGRPCController(c.ctx, c.gRPCClients, fn)
			c.grpc[name] = controller
		}
		controller.Stop()
		controller.SetQPS(cfg.QPS)
		if cfg.Burst > 0 {
			controller.SetBurst(cfg.Burst)
		}
		controller.Run()
	} else {
		return errors.Errorf("HTTP case %s not implemented", name)
	}
	return nil
}

type HTTPController struct {
	HTTPCase
	clients []pdHttp.Client
	pctx    context.Context

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewHTTPController(ctx context.Context, clis []pdHttp.Client, fn HTTPCraeteFn) *HTTPController {
	c := &HTTPController{
		pctx:     ctx,
		clients:  clis,
		HTTPCase: fn(),
	}
	return c
}

func (c *HTTPController) Run() {
	if c.GetQPS() <= 0 || c.cancel != nil {
		return
	}
	c.ctx, c.cancel = context.WithCancel(c.pctx)
	qps := c.GetQPS()
	burst := c.GetBurst()
	cliNum := int64(len(c.clients))
	tt := time.Duration(base/qps*burst*cliNum) * time.Microsecond
	log.Info("begin to run http case", zap.String("case", c.Name()), zap.Int64("qps", qps), zap.Int64("burst", burst), zap.Duration("interval", tt))
	for _, hCli := range c.clients {
		c.wg.Add(1)
		go func(hCli pdHttp.Client) {
			defer c.wg.Done()
			var ticker = time.NewTicker(tt)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					for i := int64(0); i < burst; i++ {
						err := c.Do(c.ctx, hCli)
						if err != nil {
							log.Error("meet erorr when doing HTTP request", zap.String("case", c.Name()), zap.Error(err))
						}
					}
				case <-c.ctx.Done():
					log.Info("Got signal to exit handleScanRegions")
					return
				}
			}
		}(hCli)
	}
}

func (c *HTTPController) Stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	c.cancel = nil
	c.wg.Wait()
}

type GRPCController struct {
	GRPCCase
	clients []pd.Client
	pctx    context.Context

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
}

func NewGRPCController(ctx context.Context, clis []pd.Client, fn GRPCCraeteFn) *GRPCController {
	c := &GRPCController{
		pctx:     ctx,
		clients:  clis,
		GRPCCase: fn(),
	}
	return c
}

func (c *GRPCController) Run() {
	if c.GetQPS() <= 0 || c.cancel != nil {
		return
	}
	c.ctx, c.cancel = context.WithCancel(c.pctx)
	qps := c.GetQPS()
	burst := c.GetBurst()
	cliNum := int64(len(c.clients))
	tt := time.Duration(base/qps*burst*cliNum) * time.Microsecond
	log.Info("begin to run gRPC case", zap.String("case", c.Name()), zap.Int64("qps", qps), zap.Int64("burst", burst), zap.Duration("interval", tt))
	for _, cli := range c.clients {
		c.wg.Add(1)
		go func(cli pd.Client) {
			defer c.wg.Done()
			var ticker = time.NewTicker(tt)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					for i := int64(0); i < burst; i++ {
						err := c.Unary(c.ctx, cli)
						if err != nil {
							log.Error("meet erorr when doing gRPC request", zap.String("case", c.Name()), zap.Error(err))
						}
					}
				case <-c.ctx.Done():
					log.Info("Got signal to exit handleGetRegion")
					return
				}
			}
		}(cli)
	}
}

func (c *GRPCController) Stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	c.cancel = nil
	c.wg.Wait()
}
