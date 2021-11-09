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

	"github.com/pingcap/errors"
	"google.golang.org/grpc"
)

const (
	defaultPDTimeout                             = 3 * time.Second
	maxInitClusterRetries                        = 100
	defaultMaxTSOBatchWaitInterval time.Duration = 0
	defaultEnableTSOFollowerProxy                = false
)

// DynamicOption is used to distinguish the dynamic option type.
type DynamicOption int

const (
	// MaxTSOBatchWaitInterval is the max TSO batch wait interval option.
	// It is stored as time.Duration and should be between 0 and 10ms.
	MaxTSOBatchWaitInterval DynamicOption = iota
	// EnableTSOFollowerProxy is the TSO Follower Proxy option.
	// It is stored as bool.
	EnableTSOFollowerProxy

	dynamicOptionCount
)

// option is the configurable option for the PD client.
// It provides the ability to change some PD client's options online from the outside.
type option struct {
	// Static options.
	gRPCDialOptions  []grpc.DialOption
	timeout          time.Duration
	maxRetryTimes    int
	enableForwarding bool

	// Dynamic options.
	dynamicOptions [dynamicOptionCount]atomic.Value

	enableTSOFollowerProxyCh chan struct{}
}

// newOption creates a new PD client option with the default values set.
func newOption() *option {
	co := &option{
		timeout:                  defaultPDTimeout,
		maxRetryTimes:            maxInitClusterRetries,
		enableTSOFollowerProxyCh: make(chan struct{}, 1),
	}

	co.dynamicOptions[MaxTSOBatchWaitInterval].Store(defaultMaxTSOBatchWaitInterval)
	co.dynamicOptions[EnableTSOFollowerProxy].Store(defaultEnableTSOFollowerProxy)
	return co
}

// setMaxTSOBatchWaitInterval sets the max TSO batch wait interval option.
// It only accepts the interval value between 0 and 10ms.
func (o *option) setMaxTSOBatchWaitInterval(interval time.Duration) error {
	if interval < 0 || interval > 10*time.Millisecond {
		return errors.New("[pd] invalid max TSO batch wait interval, should be between 0 and 10ms")
	}
	old := o.getMaxTSOBatchWaitInterval()
	if interval != old {
		o.dynamicOptions[MaxTSOBatchWaitInterval].Store(interval)
	}
	return nil
}

// getMaxTSOBatchWaitInterval gets the max TSO batch wait interval option.
func (o *option) getMaxTSOBatchWaitInterval() time.Duration {
	return o.dynamicOptions[MaxTSOBatchWaitInterval].Load().(time.Duration)
}

// setEnableTSOFollowerProxy sets the TSO Follower Proxy option.
func (o *option) setEnableTSOFollowerProxy(enable bool) {
	old := o.getEnableTSOFollowerProxy()
	if enable != old {
		o.dynamicOptions[EnableTSOFollowerProxy].Store(enable)
		select {
		case o.enableTSOFollowerProxyCh <- struct{}{}:
		default:
		}
	}
}

// getEnableTSOFollowerProxy gets the TSO Follower Proxy option.
func (o *option) getEnableTSOFollowerProxy() bool {
	return o.dynamicOptions[EnableTSOFollowerProxy].Load().(bool)
}
