// Copyright 2016 TiKV Project Authors.
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

package id

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

type label string

const (
	// DefaultLabel is the default label for id allocator.
	DefaultLabel label = "idalloc"
	// KeyspaceLabel is the label for keyspace id allocator.
	KeyspaceLabel label = "keyspace-idAlloc"

	// DefaultAllocStep is the default step size for id allocation.
	DefaultAllocStep = uint64(1000)
)

// Allocator is the allocator to generate unique ID.
type Allocator interface {
	// SetBase set base id
	SetBase(newBase uint64) error
	// Alloc allocs a unique id.
	Alloc(count uint32) (uint64, uint32, error)
	// Rebase resets the base for the allocator from the persistent window boundary,
	// which also resets the end of the allocator. (base, end) is the range that can
	// be allocated in memory.
	Rebase() error
}

// allocatorImpl is used to allocate ID.
type allocatorImpl struct {
	mu   syncutil.RWMutex
	base uint64
	end  uint64

	client       *clientv3.Client
	label        label
	member       string
	step         uint64
	metrics      *metrics
	effectiveEnd uint64
}

// metrics is a collection of idAllocator's metrics.
type metrics struct {
	idGauge prometheus.Gauge
}

// AllocatorParams are parameters needed to create a new ID Allocator.
type AllocatorParams struct {
	Client *clientv3.Client
	Label  label  // Label used to label metrics and logs.
	Member string // Member value, used to check if current pd leader.
	Step   uint64 // Step size of each persistent window boundary increment, default 1000.
}

// NewAllocator creates a new ID Allocator.
func NewAllocator(params *AllocatorParams) Allocator {
	allocator := &allocatorImpl{
		client:  params.Client,
		label:   params.Label,
		member:  params.Member,
		step:    params.Step,
		metrics: &metrics{idGauge: idGauge.WithLabelValues(string(params.Label))},
	}
	if allocator.step == 0 {
		allocator.step = DefaultAllocStep
	}
	var effectiveEnd uint64
	effectiveEnd = math.MaxUint64
	if params.Label == KeyspaceLabel {
		if kerneltype.IsNextGen() {
			effectiveEnd = constant.ReservedKeyspaceIDStart - 1 // Last allocable ID for NextGen
		} else {
			effectiveEnd = uint64(constant.MaxValidKeyspaceID) // Last allocable ID for non NextGen
		}
	}
	allocator.effectiveEnd = effectiveEnd
	return allocator
}

// Alloc returns a new id.
func (alloc *allocatorImpl) Alloc(count uint32) (uint64, uint32, error) {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	if uint64(count) > alloc.end-alloc.base {
		if alloc.base >= alloc.effectiveEnd || uint64(count) > alloc.effectiveEnd-alloc.base {
			return 0, 0, errs.ErrIDExhausted.FastGenByArgs()
		}
		if err := alloc.rebaseLocked(true, count); err != nil {
			return 0, 0, err
		}
	}
	alloc.base += uint64(count)

	return alloc.base, count, nil
}

// SetBase sets the base.
func (alloc *allocatorImpl) SetBase(newBase uint64) error {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	// Ensure the newBase is valid.
	if newBase >= alloc.effectiveEnd {
		return errs.ErrIDExhausted.FastGenByArgs()
	}
	previousEnd := alloc.end
	// set current end to new base, rebaseLocked will change it later.
	alloc.end = newBase
	if err := alloc.rebaseLocked(false, 0); err != nil {
		alloc.end = previousEnd
		return err
	}
	return nil
}

// Rebase resets the base for the allocator from the persistent window boundary,
// which also resets the end of the allocator. (base, end) is the range that can
// be allocated in memory.
func (alloc *allocatorImpl) Rebase() error {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	return alloc.rebaseLocked(true, 0)
}

func (alloc *allocatorImpl) rebaseLocked(checkCurrEnd bool, count uint32) error {
	var key string
	if alloc.label == KeyspaceLabel {
		key = keypath.KeyspaceAllocIDPath()
	} else {
		key = keypath.AllocIDPath()
	}

	leaderPath := keypath.ElectionPath(nil)
	var (
		cmps = []clientv3.Cmp{clientv3.Compare(clientv3.Value(leaderPath), "=", alloc.member)}
		end  uint64
	)

	if checkCurrEnd {
		value, err := etcdutil.GetValue(alloc.client, key)
		if err != nil {
			return err
		}
		if value == nil {
			// create the key
			cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(key), "=", 0))
		} else {
			// update the key
			end, err = typeutil.BytesToUint64(value)
			if err != nil {
				return err
			}

			cmps = append(cmps, clientv3.Compare(clientv3.Value(key), "=", string(value)))
		}
	} else {
		end = alloc.end
	}

	base := end
	if count != 0 && end == alloc.end {
		// Extend our current window only if no other allocator reserved IDs
		// after it. Otherwise discard the remainder to keep the batch contiguous.
		base = alloc.base
	}
	required := max(uint64(count), 1)
	if end >= alloc.effectiveEnd || required > alloc.effectiveEnd-base {
		return errs.ErrIDExhausted.FastGenByArgs()
	}
	needed := required - (end - base)
	steps := (needed-1)/alloc.step + 1
	newEnd := alloc.effectiveEnd
	if steps <= (alloc.effectiveEnd-end)/alloc.step {
		newEnd = end + steps*alloc.step
	}

	value := typeutil.Uint64ToBytes(newEnd)
	txn := kv.NewSlowLogTxn(alloc.client)
	resp, err := txn.If(cmps...).Then(clientv3.OpPut(key, string(value))).Commit()
	if err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByArgs()
	}
	if !resp.Succeeded {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}

	alloc.metrics.idGauge.Set(float64(newEnd))
	alloc.end = newEnd
	alloc.base = base
	// please do not reorder the first field, it's need when getting the new-end
	// see: https://docs.pingcap.com/tidb/dev/pd-recover#get-allocated-id-from-pd-log
	log.Info("idAllocator allocates a new id", zap.Uint64("new-end", newEnd), zap.Uint64("new-base", alloc.base),
		zap.String("label", string(alloc.label)), zap.Bool("check-curr-end", checkCurrEnd))
	return nil
}
