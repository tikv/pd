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

package pd

import (
	"context"
	"runtime/trace"
	"sync"
	"time"

	"github.com/pingcap/errors"
)

// TSFuture is a future which promises to return a TSO.
type TSFuture interface {
	// Wait gets the physical and logical time, it would block caller if data is not available yet.
	Wait() (int64, int64, error)
}

var (
	_ TSFuture = (*tsoRequest)(nil)
	_ TSFuture = (*tsoRequestFastFail)(nil)
)

type tsoRequest struct {
	requestCtx context.Context
	clientCtx  context.Context
	done       chan error
	physical   int64
	logical    int64
	dcLocation string

	// Runtime fields.
	start time.Time
	pool  *sync.Pool
}

// tryDone tries to send the result to the channel, it will not block.
func (req *tsoRequest) tryDone(err error) {
	select {
	case req.done <- err:
	default:
	}
}

// Wait will block until the TSO result is ready.
func (req *tsoRequest) Wait() (physical int64, logical int64, err error) {
	// If tso command duration is observed very high, the reason could be it
	// takes too long for Wait() be called.
	start := time.Now()
	cmdDurationTSOAsyncWait.Observe(start.Sub(req.start).Seconds())
	select {
	case err = <-req.done:
		defer trace.StartRegion(req.requestCtx, "pdclient.tsoReqDone").End()
		defer req.pool.Put(req)
		err = errors.WithStack(err)
		if err != nil {
			cmdFailDurationTSO.Observe(time.Since(req.start).Seconds())
			return 0, 0, err
		}
		physical, logical = req.physical, req.logical
		now := time.Now()
		cmdDurationWait.Observe(now.Sub(start).Seconds())
		cmdDurationTSO.Observe(now.Sub(req.start).Seconds())
		return
	case <-req.requestCtx.Done():
		return 0, 0, errors.WithStack(req.requestCtx.Err())
	case <-req.clientCtx.Done():
		return 0, 0, errors.WithStack(req.clientCtx.Err())
	}
}

type tsoRequestFastFail struct {
	err error
}

func newTSORequestFastFail(err error) *tsoRequestFastFail {
	return &tsoRequestFastFail{err}
}

// Wait returns the error directly.
func (req *tsoRequestFastFail) Wait() (physical int64, logical int64, err error) {
	return 0, 0, req.err
}
