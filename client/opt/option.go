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

package opt

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/client/pkg/retry"
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

// Option is the configurable option for the PD client.
// It provides the ability to change some PD client's options online from the outside.
type Option struct {
	// Static options.
	GRPCDialOptions   []grpc.DialOption
	Timeout           time.Duration
	MaxRetryTimes     int
	EnableForwarding  bool
	UseTSOServerProxy bool
	MetricsLabels     prometheus.Labels
	InitMetrics       bool
	Backoffer         *retry.Backoffer

	// Dynamic options.
	dynamicOptions [dynamicOptionCount]atomic.Value

	EnableTSOFollowerProxyCh chan struct{}
}

// NewOption creates a new PD client option with the default values set.
func NewOption() *Option {
	co := &Option{
		Timeout:                  defaultPDTimeout,
		MaxRetryTimes:            maxInitClusterRetries,
		EnableTSOFollowerProxyCh: make(chan struct{}, 1),
		InitMetrics:              true,
	}

	co.dynamicOptions[MaxTSOBatchWaitInterval].Store(defaultMaxTSOBatchWaitInterval)
	co.dynamicOptions[EnableTSOFollowerProxy].Store(defaultEnableTSOFollowerProxy)
	co.dynamicOptions[EnableFollowerHandle].Store(defaultEnableFollowerHandle)
	co.dynamicOptions[TSOClientRPCConcurrency].Store(defaultTSOClientRPCConcurrency)
	return co
}

// SetMaxTSOBatchWaitInterval sets the max TSO batch wait interval option.
// It only accepts the interval value between 0 and 10ms.
func (o *Option) SetMaxTSOBatchWaitInterval(interval time.Duration) error {
	if interval < 0 || interval > 10*time.Millisecond {
		return errors.New("[pd] invalid max TSO batch wait interval, should be between 0 and 10ms")
	}
	old := o.GetMaxTSOBatchWaitInterval()
	if interval != old {
		o.dynamicOptions[MaxTSOBatchWaitInterval].Store(interval)
	}
	return nil
}

// SetEnableFollowerHandle set the Follower Handle option.
func (o *Option) SetEnableFollowerHandle(enable bool) {
	old := o.GetEnableFollowerHandle()
	if enable != old {
		o.dynamicOptions[EnableFollowerHandle].Store(enable)
	}
}

// GetEnableFollowerHandle gets the Follower Handle enable option.
func (o *Option) GetEnableFollowerHandle() bool {
	return o.dynamicOptions[EnableFollowerHandle].Load().(bool)
}

// GetMaxTSOBatchWaitInterval gets the max TSO batch wait interval option.
func (o *Option) GetMaxTSOBatchWaitInterval() time.Duration {
	return o.dynamicOptions[MaxTSOBatchWaitInterval].Load().(time.Duration)
}

// SetEnableTSOFollowerProxy sets the TSO Follower Proxy option.
func (o *Option) SetEnableTSOFollowerProxy(enable bool) {
	old := o.GetEnableTSOFollowerProxy()
	if enable != old {
		o.dynamicOptions[EnableTSOFollowerProxy].Store(enable)
		select {
		case o.EnableTSOFollowerProxyCh <- struct{}{}:
		default:
		}
	}
}

// GetEnableTSOFollowerProxy gets the TSO Follower Proxy option.
func (o *Option) GetEnableTSOFollowerProxy() bool {
	return o.dynamicOptions[EnableTSOFollowerProxy].Load().(bool)
}

// SetTSOClientRPCConcurrency sets the TSO client RPC concurrency option.
func (o *Option) SetTSOClientRPCConcurrency(value int) {
	old := o.GetTSOClientRPCConcurrency()
	if value != old {
		o.dynamicOptions[TSOClientRPCConcurrency].Store(value)
	}
}

// GetTSOClientRPCConcurrency gets the TSO client RPC concurrency option.
func (o *Option) GetTSOClientRPCConcurrency() int {
	return o.dynamicOptions[TSOClientRPCConcurrency].Load().(int)
}

// ClientOption configures client.
type ClientOption func(*Option)

// WithGRPCDialOptions configures the client with gRPC dial options.
func WithGRPCDialOptions(opts ...grpc.DialOption) ClientOption {
	return func(op *Option) {
		op.GRPCDialOptions = append(op.GRPCDialOptions, opts...)
	}
}

// WithCustomTimeoutOption configures the client with timeout option.
func WithCustomTimeoutOption(timeout time.Duration) ClientOption {
	return func(op *Option) {
		op.Timeout = timeout
	}
}

// WithForwardingOption configures the client with forwarding option.
func WithForwardingOption(enableForwarding bool) ClientOption {
	return func(op *Option) {
		op.EnableForwarding = enableForwarding
	}
}

// WithTSOServerProxyOption configures the client to use TSO server proxy,
// i.e., the client will send TSO requests to the API leader (the TSO server
// proxy) which will forward the requests to the TSO servers.
func WithTSOServerProxyOption(useTSOServerProxy bool) ClientOption {
	return func(op *Option) {
		op.UseTSOServerProxy = useTSOServerProxy
	}
}

// WithMaxErrorRetry configures the client max retry times when connect meets error.
func WithMaxErrorRetry(count int) ClientOption {
	return func(op *Option) {
		op.MaxRetryTimes = count
	}
}

// WithMetricsLabels configures the client with metrics labels.
func WithMetricsLabels(labels prometheus.Labels) ClientOption {
	return func(op *Option) {
		op.MetricsLabels = labels
	}
}

// WithInitMetricsOption configures the client with metrics labels.
func WithInitMetricsOption(initMetrics bool) ClientOption {
	return func(op *Option) {
		op.InitMetrics = initMetrics
	}
}

// WithBackoffer configures the client with backoffer.
func WithBackoffer(bo *retry.Backoffer) ClientOption {
	return func(op *Option) {
		op.Backoffer = bo
	}
}

// GetStoreOp represents available options when getting stores.
type GetStoreOp struct {
	ExcludeTombstone bool
}

// GetStoreOption configures GetStoreOp.
type GetStoreOption func(*GetStoreOp)

// WithExcludeTombstone excludes tombstone stores from the result.
func WithExcludeTombstone() GetStoreOption {
	return func(op *GetStoreOp) { op.ExcludeTombstone = true }
}

// RegionsOp represents available options when operate regions
type RegionsOp struct {
	Group          string
	RetryLimit     uint64
	SkipStoreLimit bool
}

// RegionsOption configures RegionsOp
type RegionsOption func(op *RegionsOp)

// WithGroup specify the group during Scatter/Split Regions
func WithGroup(group string) RegionsOption {
	return func(op *RegionsOp) { op.Group = group }
}

// WithRetry specify the retry limit during Scatter/Split Regions
func WithRetry(retry uint64) RegionsOption {
	return func(op *RegionsOp) { op.RetryLimit = retry }
}

// WithSkipStoreLimit specify if skip the store limit check during Scatter/Split Regions
func WithSkipStoreLimit() RegionsOption {
	return func(op *RegionsOp) { op.SkipStoreLimit = true }
}

// GetRegionOp represents available options when getting regions.
type GetRegionOp struct {
	NeedBuckets                  bool
	AllowFollowerHandle          bool
	OutputMustContainAllKeyRange bool
}

// GetRegionOption configures GetRegionOp.
type GetRegionOption func(op *GetRegionOp)

// WithBuckets means getting region and its buckets.
func WithBuckets() GetRegionOption {
	return func(op *GetRegionOp) { op.NeedBuckets = true }
}

// WithAllowFollowerHandle means that client can send request to follower and let it handle this request.
func WithAllowFollowerHandle() GetRegionOption {
	return func(op *GetRegionOp) { op.AllowFollowerHandle = true }
}

// WithOutputMustContainAllKeyRange means the output must contain all key ranges.
func WithOutputMustContainAllKeyRange() GetRegionOption {
	return func(op *GetRegionOp) { op.OutputMustContainAllKeyRange = true }
}

// MetaStorageOp represents available options when using meta storage client.
type MetaStorageOp struct {
	RangeEnd         []byte
	Revision         int64
	PrevKv           bool
	Lease            int64
	Limit            int64
	IsOptsWithPrefix bool
}

// MetaStorageOption configures MetaStorageOp.
type MetaStorageOption func(*MetaStorageOp)

// WithLimit specifies the limit of the key.
func WithLimit(limit int64) MetaStorageOption {
	return func(op *MetaStorageOp) { op.Limit = limit }
}

// WithRangeEnd specifies the range end of the key.
func WithRangeEnd(rangeEnd []byte) MetaStorageOption {
	return func(op *MetaStorageOp) { op.RangeEnd = rangeEnd }
}

// WithRev specifies the start revision of the key.
func WithRev(revision int64) MetaStorageOption {
	return func(op *MetaStorageOp) { op.Revision = revision }
}

// WithPrevKV specifies the previous key-value pair of the key.
func WithPrevKV() MetaStorageOption {
	return func(op *MetaStorageOp) { op.PrevKv = true }
}

// WithLease specifies the lease of the key.
func WithLease(lease int64) MetaStorageOption {
	return func(op *MetaStorageOp) { op.Lease = lease }
}

// WithPrefix specifies the prefix of the key.
func WithPrefix() MetaStorageOption {
	return func(op *MetaStorageOp) {
		op.IsOptsWithPrefix = true
	}
}
