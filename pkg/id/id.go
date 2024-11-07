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
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type label string

const (
	// DefaultLabel is the default label for id allocator.
	DefaultLabel label = "idalloc"
	// KeyspaceLabel is the label for keyspace id allocator.
	KeyspaceLabel label = "keyspace-idAlloc"
)

// Allocator is the allocator to generate unique ID.
type Allocator interface {
	// SetBase set base id
	SetBase(newBase uint64) error
	// Alloc allocs a unique id.
	Alloc() (uint64, error)
	// Rebase resets the base for the allocator from the persistent window boundary,
	// which also resets the end of the allocator. (base, end) is the range that can
	// be allocated in memory.
	Rebase() error
}

const defaultAllocStep = uint64(1000)

// allocatorImpl is used to allocate ID.
type allocatorImpl struct {
	mu   syncutil.RWMutex
	base uint64
	end  uint64

	client  *clientv3.Client
	label   label
	member  string
	step    uint64
	metrics *metrics
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
		allocator.step = defaultAllocStep
	}
	return allocator
}

// Alloc returns a new id.
func (alloc *allocatorImpl) Alloc() (uint64, error) {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	if alloc.base == alloc.end {
		if err := alloc.rebaseLocked(true); err != nil {
			return 0, err
		}
	}

	alloc.base++

	return alloc.base, nil
}

// SetBase sets the base.
func (alloc *allocatorImpl) SetBase(newBase uint64) error {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	// set current end to new base, rebaseLocked will change it later.
	alloc.end = newBase

	return alloc.rebaseLocked(false)
}

// Rebase resets the base for the allocator from the persistent window boundary,
// which also resets the end of the allocator. (base, end) is the range that can
// be allocated in memory.
func (alloc *allocatorImpl) Rebase() error {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	return alloc.rebaseLocked(true)
}

func (alloc *allocatorImpl) rebaseLocked(checkCurrEnd bool) error {
	var key string
	if alloc.label == KeyspaceLabel {
		key = keypath.KeyspaceAllocIDPath()
	} else {
		key = keypath.AllocIDPath()
	}

	leaderPath := keypath.LeaderPath(nil)
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

	end += alloc.step
	value := typeutil.Uint64ToBytes(end)
	txn := kv.NewSlowLogTxn(alloc.client)
	resp, err := txn.If(cmps...).Then(clientv3.OpPut(key, string(value))).Commit()
	if err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByArgs()
	}
	if !resp.Succeeded {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}

	alloc.metrics.idGauge.Set(float64(end))
	alloc.end = end
	alloc.base = end - alloc.step
	// please do not reorder the first field, it's need when getting the new-end
	// see: https://docs.pingcap.com/tidb/dev/pd-recover#get-allocated-id-from-pd-log
	log.Info("idAllocator allocates a new id", zap.Uint64("new-end", end), zap.Uint64("new-base", alloc.base),
		zap.String("label", string(alloc.label)), zap.Bool("check-curr-end", checkCurrEnd))
	return nil
}
