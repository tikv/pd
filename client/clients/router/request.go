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

	"github.com/pingcap/errors"
)

// RegionFuture is a future which promises to return a region info.
type RegionFuture interface {
	// Wait gets the region info, it would block caller if data is not available yet.
	Wait() (*Region, error)
}

var _ RegionFuture = (*Request)(nil)

// Request is a region info request.
type Request struct {
	requestCtx context.Context
	clientCtx  context.Context
	done       chan error
	region     *Region

	// Runtime fields.
	pool *sync.Pool
}

// TryDone tries to send the result to the channel, it will not block.
func (req *Request) TryDone(err error) {
	select {
	case req.done <- err:
	default:
	}
}

// Wait will block until the region info is ready.
func (req *Request) Wait() (*Region, error) {
	return req.waitCtx(req.requestCtx)
}

// waitCtx waits for the region info with specified ctx, while not using req.requestCtx.
func (req *Request) waitCtx(ctx context.Context) (region *Region, err error) {
	// TODO: introduce the metrics.
	select {
	case err = <-req.done:
		defer req.pool.Put(req)
		defer trace.StartRegion(req.requestCtx, "pdclient.regionReqDone").End()
		err = errors.WithStack(err)
		if err != nil {
			return nil, err
		}
		return req.region, nil
	case <-ctx.Done():
		return nil, errors.WithStack(ctx.Err())
	case <-req.clientCtx.Done():
		return nil, errors.WithStack(req.clientCtx.Err())
	}
}
