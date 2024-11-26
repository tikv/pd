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

package tso

import (
	"context"
	"runtime/trace"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/tikv/pd/client/metrics"
)

// TSFuture is a future which promises to return a TSO.
type TSFuture interface {
	// Wait gets the physical and logical time, it would block caller if data is not available yet.
	Wait() (int64, int64, error)
}

var (
	_ TSFuture = (*Request)(nil)
	_ TSFuture = (*tsoRequestFastFail)(nil)
)

// Request is a TSO request.
type Request struct {
	requestCtx context.Context
	clientCtx  context.Context
	done       chan error
	physical   int64
	logical    int64

	// The identifier of the RPC stream in which the request is processed.
	streamID string

	// Runtime fields.
	start time.Time
	pool  *sync.Pool
}

// IsFrom checks if the request is from the specified pool.
func (req *Request) IsFrom(pool *sync.Pool) bool {
	if req == nil {
		return false
	}
	return req.pool == pool
}

// TryDone tries to send the result to the channel, it will not block.
func (req *Request) TryDone(err error) {
	select {
	case req.done <- err:
	default:
	}
}

// Wait will block until the TSO result is ready.
func (req *Request) Wait() (physical int64, logical int64, err error) {
	return req.waitCtx(req.requestCtx)
}

// waitCtx waits for the TSO result with specified ctx, while not using req.requestCtx.
func (req *Request) waitCtx(ctx context.Context) (physical int64, logical int64, err error) {
	// If tso command duration is observed very high, the reason could be it
	// takes too long for Wait() be called.
	start := time.Now()
	metrics.CmdDurationTSOAsyncWait.Observe(start.Sub(req.start).Seconds())
	select {
	case err = <-req.done:
		defer req.pool.Put(req)
		defer trace.StartRegion(req.requestCtx, "pdclient.tsoReqDone").End()
		err = errors.WithStack(err)
		now := time.Now()
		if err != nil {
			metrics.CmdFailedDurationTSOWait.Observe(now.Sub(start).Seconds())
			metrics.CmdFailedDurationTSO.Observe(now.Sub(req.start).Seconds())
			return 0, 0, err
		}
		physical, logical = req.physical, req.logical
		metrics.CmdDurationTSOWait.Observe(now.Sub(start).Seconds())
		metrics.CmdDurationTSO.Observe(now.Sub(req.start).Seconds())
		return
	case <-ctx.Done():
		return 0, 0, errors.WithStack(ctx.Err())
	case <-req.clientCtx.Done():
		return 0, 0, errors.WithStack(req.clientCtx.Err())
	}
}

// waitTimeout waits for the TSO result for limited time. Currently only for test purposes.
func (req *Request) waitTimeout(timeout time.Duration) (physical int64, logical int64, err error) {
	ctx, cancel := context.WithTimeout(req.requestCtx, timeout)
	defer cancel()
	return req.waitCtx(ctx)
}

type tsoRequestFastFail struct {
	err error
}

// NewRequestFastFail creates a new fast fail TSO request.
func NewRequestFastFail(err error) *tsoRequestFastFail {
	return &tsoRequestFastFail{err}
}

// Wait returns the error directly.
func (req *tsoRequestFastFail) Wait() (physical int64, logical int64, err error) {
	return 0, 0, req.err
}
