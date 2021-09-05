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
	"path"
	"sync"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/kv"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// Allocator is the allocator to generate unique ID.
type Allocator interface {
	// Alloc allocs a unique id.
	Alloc() (uint64, error)
	// Rebase resets the base for the allocator from the persistent window boundary,
	// which also resets the end of the allocator. (base, end) is the range that can
	// be allocated in memory.
	Rebase() error
}

const allocStep = uint64(1000)

// allocatorImpl is used to allocate ID.
type allocatorImpl struct {
	mu   sync.Mutex
	base uint64
	end  uint64

	client   *clientv3.Client
	rootPath string
	member   string
}

// NewAllocator creates a new ID Allocator.
func NewAllocator(client *clientv3.Client, rootPath string, member string) Allocator {
	return &allocatorImpl{client: client, rootPath: rootPath, member: member}
}

// Alloc returns a new id.
func (alloc *allocatorImpl) Alloc() (uint64, error) {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	if alloc.base == alloc.end {
		if err := alloc.rebaseLocked(); err != nil {
			return 0, err
		}
	}

	alloc.base++

	return alloc.base, nil
}

// Rebase resets the base for the allocator from the persistent window boundary,
// which also resets the end of the allocator. (base, end) is the range that can
// be allocated in memory.
func (alloc *allocatorImpl) Rebase() error {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	return alloc.rebaseLocked()
}

func (alloc *allocatorImpl) rebaseLocked() error {
	key := alloc.getAllocIDPath()
	value, err := etcdutil.GetValue(alloc.client, key)
	if err != nil {
		return err
	}

	var (
		cmp clientv3.Cmp
		end uint64
	)

	if value == nil {
		// create the key
		cmp = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	} else {
		// update the key
		end, err = typeutil.BytesToUint64(value)
		if err != nil {
			return err
		}

		cmp = clientv3.Compare(clientv3.Value(key), "=", string(value))
	}

	end += allocStep
	value = typeutil.Uint64ToBytes(end)
	txn := kv.NewSlowLogTxn(alloc.client)
	leaderPath := path.Join(alloc.rootPath, "leader")
	t := txn.If(append([]clientv3.Cmp{cmp}, clientv3.Compare(clientv3.Value(leaderPath), "=", alloc.member))...)
	resp, err := t.Then(clientv3.OpPut(key, string(value))).Commit()
	if err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByArgs()
	}
	if !resp.Succeeded {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}

	log.Info("idAllocator allocates a new id", zap.Uint64("alloc-id", end))
	idGauge.WithLabelValues("idalloc").Set(float64(end))
	alloc.end = end
	alloc.base = end - allocStep
	return nil
}

func (alloc *allocatorImpl) getAllocIDPath() string {
	return path.Join(alloc.rootPath, "alloc_id")
}
