// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"sync"

	log "github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	allocStep = uint64(1000)
)

type idAllocator struct {
	mu   sync.Mutex
	base uint64
	end  uint64

	s *Server
}

func (alloc *idAllocator) Alloc() (uint64, error) {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	if alloc.base == alloc.end {
		if err := alloc.generateLocked(); err != nil {
			return 0, err
		}
	}

	alloc.base++

	return alloc.base, nil
}

// Generate synchronizes and generates id range.
func (alloc *idAllocator) Generate() error {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	return alloc.generateLocked()
}

func (alloc *idAllocator) generateLocked() error {
	key := alloc.s.getAllocIDPath()
	value, err := getValue(alloc.s.client, key)
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
		end, err = bytesToUint64(value)
		if err != nil {
			return err
		}

		cmp = clientv3.Compare(clientv3.Value(key), "=", string(value))
	}

	end += allocStep
	value = uint64ToBytes(end)
	resp, err := alloc.s.leaderTxn(cmp).Then(clientv3.OpPut(key, string(value))).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return errors.New("generate id failed, we may not leader")
	}

	log.Info("idAllocator allocates a new id", zap.Uint64("alloc-id", end))
	metadataGauge.WithLabelValues("idalloc").Set(float64(end))
	alloc.end = end
	alloc.base = end - allocStep
	return nil
}
