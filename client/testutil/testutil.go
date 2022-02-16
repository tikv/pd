// Copyright 2022 TiKV Project Authors.
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

package testutil

import (
	"time"

	"github.com/pingcap/check"
)

const (
	waitMaxRetry   = 200
	waitRetrySleep = time.Millisecond * 100
)

// WaitOp represents available options when execute WaitUntil
type WaitOp struct {
	retryTimes    int
	sleepInterval time.Duration
}

// WaitOption configures WaitOp
type WaitOption func(op *WaitOp)

// WithRetryTimes specify the retry times
func WithRetryTimes(retryTimes int) WaitOption {
	return func(op *WaitOp) { op.retryTimes = retryTimes }
}

// WithSleepInterval specify the sleep duration
func WithSleepInterval(sleep time.Duration) WaitOption {
	return func(op *WaitOp) { op.sleepInterval = sleep }
}

// WaitUntil repeatedly evaluates f() for a period of time, util it returns true.
func WaitUntil(c *check.C, f func() bool, opts ...WaitOption) {
	c.Log("wait start")
	option := &WaitOp{
		retryTimes:    waitMaxRetry,
		sleepInterval: waitRetrySleep,
	}
	for _, opt := range opts {
		opt(option)
	}
	for i := 0; i < option.retryTimes; i++ {
		if f() {
			return
		}
		time.Sleep(option.sleepInterval)
	}
	c.Fatal("wait timeout")
}
