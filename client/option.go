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
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

const (
	defaultPDTimeout                             = 3 * time.Second
	maxInitClusterRetries                        = 100
	defaultMaxTSOBatchWaitInterval time.Duration = 0
	defaultEnableTSOFollowerProxy                = false
	defaultEnableFollowerHandle                  = false
	defaultTSOClientRPCConcurrency               = 1
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
	// EnableFollowerHandle is the follower handle option.
	EnableFollowerHandle
	// TSOClientRPCConcurrency controls the amount of ongoing TSO RPC requests at the same time in a single TSO client.
	TSOClientRPCConcurrency

	dynamicOptionCount
)

// option is the configurable option for the PD client.
// It provides the ability to change some PD client's options online from the outside.
type option struct {
	// Static options.
	gRPCDialOptions   []grpc.DialOption
	timeout           time.Duration
	maxRetryTimes     int
	enableForwarding  bool
	useTSOServerProxy bool
	metricsLabels     prometheus.Labels
	initMetrics       bool

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
		initMetrics:              true,
	}

	co.dynamicOptions[MaxTSOBatchWaitInterval].Store(defaultMaxTSOBatchWaitInterval)
	co.dynamicOptions[EnableTSOFollowerProxy].Store(defaultEnableTSOFollowerProxy)
	co.dynamicOptions[EnableFollowerHandle].Store(defaultEnableFollowerHandle)
	co.dynamicOptions[TSOClientRPCConcurrency].Store(defaultTSOClientRPCConcurrency)
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

// setEnableFollowerHandle set the Follower Handle option.
func (o *option) setEnableFollowerHandle(enable bool) {
	old := o.getEnableFollowerHandle()
	if enable != old {
		o.dynamicOptions[EnableFollowerHandle].Store(enable)
	}
}

// getMaxTSOBatchWaitInterval gets the Follower Handle enable option.
func (o *option) getEnableFollowerHandle() bool {
	return o.dynamicOptions[EnableFollowerHandle].Load().(bool)
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

func (o *option) setTSOClientRPCConcurrency(value int) {
	old := o.getTSOClientRPCConcurrency()
	if value != old {
		o.dynamicOptions[TSOClientRPCConcurrency].Store(value)
	}
}

func (o *option) getTSOClientRPCConcurrency() int {
	return o.dynamicOptions[TSOClientRPCConcurrency].Load().(int)
}

// GetStoreOp represents available options when getting stores.
type GetStoreOp struct {
	excludeTombstone bool
}

// GetStoreOption configures GetStoreOp.
type GetStoreOption func(*GetStoreOp)

// WithExcludeTombstone excludes tombstone stores from the result.
func WithExcludeTombstone() GetStoreOption {
	return func(op *GetStoreOp) { op.excludeTombstone = true }
}

// RegionsOp represents available options when operate regions
type RegionsOp struct {
	group          string
	retryLimit     uint64
	skipStoreLimit bool
}

// RegionsOption configures RegionsOp
type RegionsOption func(op *RegionsOp)

// WithGroup specify the group during Scatter/Split Regions
func WithGroup(group string) RegionsOption {
	return func(op *RegionsOp) { op.group = group }
}

// WithRetry specify the retry limit during Scatter/Split Regions
func WithRetry(retry uint64) RegionsOption {
	return func(op *RegionsOp) { op.retryLimit = retry }
}

// WithSkipStoreLimit specify if skip the store limit check during Scatter/Split Regions
func WithSkipStoreLimit() RegionsOption {
	return func(op *RegionsOp) { op.skipStoreLimit = true }
}

// GetRegionOp represents available options when getting regions.
type GetRegionOp struct {
	needBuckets                  bool
	allowFollowerHandle          bool
	outputMustContainAllKeyRange bool
}

// GetRegionOption configures GetRegionOp.
type GetRegionOption func(op *GetRegionOp)

// WithBuckets means getting region and its buckets.
func WithBuckets() GetRegionOption {
	return func(op *GetRegionOp) { op.needBuckets = true }
}

// WithAllowFollowerHandle means that client can send request to follower and let it handle this request.
func WithAllowFollowerHandle() GetRegionOption {
	return func(op *GetRegionOp) { op.allowFollowerHandle = true }
}

// WithOutputMustContainAllKeyRange means the output must contain all key ranges.
func WithOutputMustContainAllKeyRange() GetRegionOption {
	return func(op *GetRegionOp) { op.outputMustContainAllKeyRange = true }
}

// ClientOption configures client.
type ClientOption func(c *client)

// WithGRPCDialOptions configures the client with gRPC dial options.
func WithGRPCDialOptions(opts ...grpc.DialOption) ClientOption {
	return func(c *client) {
		c.option.gRPCDialOptions = append(c.option.gRPCDialOptions, opts...)
	}
}

// WithCustomTimeoutOption configures the client with timeout option.
func WithCustomTimeoutOption(timeout time.Duration) ClientOption {
	return func(c *client) {
		c.option.timeout = timeout
	}
}

// WithForwardingOption configures the client with forwarding option.
func WithForwardingOption(enableForwarding bool) ClientOption {
	return func(c *client) {
		c.option.enableForwarding = enableForwarding
	}
}

// WithTSOServerProxyOption configures the client to use TSO server proxy,
// i.e., the client will send TSO requests to the API leader (the TSO server
// proxy) which will forward the requests to the TSO servers.
func WithTSOServerProxyOption(useTSOServerProxy bool) ClientOption {
	return func(c *client) {
		c.option.useTSOServerProxy = useTSOServerProxy
	}
}

// WithMaxErrorRetry configures the client max retry times when connect meets error.
func WithMaxErrorRetry(count int) ClientOption {
	return func(c *client) {
		c.option.maxRetryTimes = count
	}
}

// WithMetricsLabels configures the client with metrics labels.
func WithMetricsLabels(labels prometheus.Labels) ClientOption {
	return func(c *client) {
		c.option.metricsLabels = labels
	}
}

// WithInitMetricsOption configures the client with metrics labels.
func WithInitMetricsOption(initMetrics bool) ClientOption {
	return func(c *client) {
		c.option.initMetrics = initMetrics
	}
}
