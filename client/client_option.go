// Copyright 2021 TiKV Project Authors.
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
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
)

const (
	defaultPDTimeout               = 3 * time.Second
	maxInitClusterRetries          = 100
	defaultMaxTSOBatchWaitInterval = 0
	defaultEnableTSOFollowerProxy  = false
)

// ClientOption is the configurable option for the PD client
type ClientOption struct {
	// Static options.
	gRPCDialOptions  []grpc.DialOption
	timeout          time.Duration
	maxRetryTimes    int
	enableForwarding bool
	// Dynamic options.
	// TODO: maybe using a more flexible way to do the dynamic registration for the new option.
	maxTSOBatchWaitInterval   atomic.Value // Store as time.Duration.
	maxTSOBatchWaitIntervalCh chan struct{}
	enableTSOFollowerProxy    atomic.Value // Store as bool.
	enableTSOFollowerProxyCh  chan struct{}
}

// NewClientOption creates a new ClientOption with the default values set.
func NewClientOption() *ClientOption {
	co := &ClientOption{
		timeout:                   defaultPDTimeout,
		maxRetryTimes:             maxInitClusterRetries,
		maxTSOBatchWaitIntervalCh: make(chan struct{}, 1),
		enableTSOFollowerProxyCh:  make(chan struct{}, 1),
	}
	co.maxTSOBatchWaitInterval.Store(time.Duration(defaultMaxTSOBatchWaitInterval))
	co.enableTSOFollowerProxy.Store(defaultEnableTSOFollowerProxy)
	return co
}

// SetMaxTSOBatchWaitInterval sets the max TSO batch wait interval option.
// It only accepts the interval value between 0 and 10ms.
func (co *ClientOption) SetMaxTSOBatchWaitInterval(interval time.Duration) {
	if interval < 0 || interval > 10*time.Millisecond {
		return
	}
	old := co.GetMaxTSOBatchWaitInterval()
	if interval != old {
		co.maxTSOBatchWaitInterval.Store(interval)
		select {
		case co.maxTSOBatchWaitIntervalCh <- struct{}{}:
		default:
		}
	}
}

// GetMaxTSOBatchWaitInterval gets the max TSO batch wait interval option.
func (co *ClientOption) GetMaxTSOBatchWaitInterval() time.Duration {
	return co.maxTSOBatchWaitInterval.Load().(time.Duration)
}

// SetEnableTSOFollowerProxy sets the TSO Follower Proxy option.
func (co *ClientOption) SetEnableTSOFollowerProxy(enable bool) {
	old := co.GetEnableTSOFollowerProxy()
	if enable != old {
		co.enableTSOFollowerProxy.Store(enable)
		select {
		case co.enableTSOFollowerProxyCh <- struct{}{}:
		default:
		}
	}
}

// GetEnableTSOFollowerProxy gets the TSO Follower Proxy option.
func (co *ClientOption) GetEnableTSOFollowerProxy() bool {
	value, ok := co.enableTSOFollowerProxy.Load().(bool)
	return ok && value
}
