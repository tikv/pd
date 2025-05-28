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

package router

import (
	"context"
	"runtime/trace"
	"sync"
	"time"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/client/metrics"
	"github.com/tikv/pd/client/opt"
)

// Request is a region info request.
type Request struct {
	requestCtx context.Context
	clientCtx  context.Context

	// Key field represents this is a `GetRegion` request.
	key []byte
	// PrevKey field represents this is a `GetPrevRegion` request.
	prevKey []byte
	// ID field represents this is a `GetRegionByID` request.
	id uint64

	options *opt.GetRegionOp

	done chan error
	// region will be set after the request is done.
	region *Region

	// Runtime fields.
	start time.Time
	pool  *sync.Pool
}

func (req *Request) tryDone(err error) {
	select {
	case req.done <- err:
	default:
	}
}

func (req *Request) wait() (*Region, error) {
	start := time.Now()
	metrics.CmdDurationQueryRegionAsyncWait.Observe(start.Sub(req.start).Seconds())
	select {
	case err := <-req.done:
		defer req.pool.Put(req)
		defer trace.StartRegion(req.requestCtx, "pdclient.regionReqDone").End()
		now := time.Now()
		if err != nil {
			metrics.CmdFailedDurationQueryRegionWait.Observe(now.Sub(start).Seconds())
			metrics.CmdFailedDurationQueryRegion.Observe(now.Sub(req.start).Seconds())
			return nil, errors.WithStack(err)
		}
		metrics.CmdDurationQueryRegionWait.Observe(now.Sub(start).Seconds())
		metrics.CmdDurationQueryRegion.Observe(now.Sub(req.start).Seconds())
		return req.region, nil
	case <-req.requestCtx.Done():
		return nil, errors.WithStack(req.requestCtx.Err())
	case <-req.clientCtx.Done():
		return nil, errors.WithStack(req.clientCtx.Err())
	}
}

// GetRegion implements the Client interface.
func (c *Cli) GetRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*Region, error) {
	req := c.newRequest(ctx, opts...)
	req.key = key

	c.requestCh <- req
	return req.wait()
}

// GetPrevRegion implements the Client interface.
func (c *Cli) GetPrevRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*Region, error) {
	req := c.newRequest(ctx, opts...)
	req.prevKey = key

	c.requestCh <- req
	return req.wait()
}

// GetRegionByID implements the Client interface.
func (c *Cli) GetRegionByID(ctx context.Context, regionID uint64, opts ...opt.GetRegionOption) (*Region, error) {
	req := c.newRequest(ctx, opts...)
	req.id = regionID

	c.requestCh <- req
	return req.wait()
}
