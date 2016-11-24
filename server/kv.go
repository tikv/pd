// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"golang.org/x/net/context"
)

const (
	kvRangeLimit      = 100000
	kvRequestTimeout  = time.Second * 10
	kvSlowRequestTime = time.Second * 1
)

var (
	errTxnFailed = errors.New("failed to commit transaction")
)

// kv wraps all kv operations, keep it stateless.
type kv struct {
	s           *Server
	client      *clientv3.Client
	clusterPath string
}

func newKV(s *Server) *kv {
	return &kv{
		s:           s,
		client:      s.client,
		clusterPath: path.Join(s.rootPath, "raft"),
	}
}

func (kv *kv) txn(cs ...clientv3.Cmp) clientv3.Txn { return kv.s.leaderTxn(cs...) }

func (kv *kv) storePath(storeID uint64) string {
	return path.Join(kv.clusterPath, "s", fmt.Sprintf("%020d", storeID))
}

func (kv *kv) regionPath(regionID uint64) string {
	return path.Join(kv.clusterPath, "r", fmt.Sprintf("%020d", regionID))
}

func (kv *kv) mergePath(regionID uint64) string {
	return path.Join(kv.clusterPath, "merge", fmt.Sprintf("%020d", regionID))
}

func (kv *kv) shutdownPath(regionID uint64) string {
	return path.Join(kv.clusterPath, "shutdown", fmt.Sprintf("%020d", regionID))
}

func (kv *kv) loadMeta(meta *metapb.Cluster) (bool, error) {
	return kv.loadProto(kv.clusterPath, meta)
}

func (kv *kv) saveMeta(meta *metapb.Cluster) error {
	return kv.saveProto(kv.clusterPath, meta)
}

func (kv *kv) loadStore(storeID uint64, store *metapb.Store) (bool, error) {
	return kv.loadProto(kv.storePath(storeID), store)
}

func (kv *kv) saveStore(store *metapb.Store) error {
	return kv.saveProto(kv.storePath(store.GetId()), store)
}

func (kv *kv) loadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	return kv.loadProto(kv.regionPath(regionID), region)
}

func (kv *kv) saveRegion(region *metapb.Region) error {
	return kv.saveProto(kv.regionPath(region.GetId()), region)
}

func (kv *kv) loadRegionMerge(fromRegionID uint64) (uint64, bool, error) {
	value, err := kv.load(kv.mergePath(1))
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if value == nil {
		return 0, false, nil
	}
	intoRegionID, err := bytesToUint64(value)
	return intoRegionID, true, errors.Trace(err)
}

func (kv *kv) markRegionMerge(fromRegionID, intoRegionID uint64) error {
	intoValue := uint64ToBytes(intoRegionID)
	return kv.save(kv.mergePath(fromRegionID), string(intoValue))
}

func (kv *kv) clearRegionMerge(fromRegionID uint64) error {
	return kv.delete(kv.mergePath(fromRegionID))
}

func (kv *kv) commitRegionMerge(fromRegionID uint64, intoRegion *metapb.Region) error {
	shutdownTime := uint64ToBytes(uint64(time.Now().Unix()))

	intoValue, err := intoRegion.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	ops := make([]clientv3.Op, 4)
	ops[0] = clientv3.OpPut(kv.shutdownPath(fromRegionID), string(shutdownTime))
	ops[1] = clientv3.OpDelete(kv.mergePath(fromRegionID))
	ops[2] = clientv3.OpDelete(kv.regionPath(fromRegionID))
	ops[3] = clientv3.OpPut(kv.regionPath(intoRegion.GetId()), string(intoValue))

	return kv.commit(ops...)
}

func (kv *kv) loadStores(stores *storesInfo, rangeLimit int64) error {
	nextID := uint64(0)
	endStore := kv.storePath(math.MaxUint64)
	withRange := clientv3.WithRange(endStore)
	withLimit := clientv3.WithLimit(rangeLimit)

	for {
		key := kv.storePath(nextID)
		resp, err := kvGet(kv.client, key, withRange, withLimit)
		if err != nil {
			return errors.Trace(err)
		}

		for _, item := range resp.Kvs {
			store := &metapb.Store{}
			if err := store.Unmarshal(item.Value); err != nil {
				return errors.Trace(err)
			}

			nextID = store.GetId() + 1
			stores.setStore(newStoreInfo(store))
		}

		if len(resp.Kvs) < int(rangeLimit) {
			return nil
		}
	}
}

func (kv *kv) loadRegions(regions *regionsInfo, rangeLimit int64) error {
	nextID := uint64(0)
	endRegion := kv.regionPath(math.MaxUint64)
	withRange := clientv3.WithRange(endRegion)
	withLimit := clientv3.WithLimit(rangeLimit)

	for {
		key := kv.regionPath(nextID)
		resp, err := kvGet(kv.client, key, withRange, withLimit)
		if err != nil {
			return errors.Trace(err)
		}

		for _, item := range resp.Kvs {
			region := &metapb.Region{}
			if err := region.Unmarshal(item.Value); err != nil {
				return errors.Trace(err)
			}

			nextID = region.GetId() + 1
			regions.setRegion(newRegionInfo(region, nil))
		}

		if len(resp.Kvs) < int(rangeLimit) {
			return nil
		}
	}
}

func (kv *kv) loadMergeRegions(regions *regionsInfo, rangeLimit int64) error {
	nextID := uint64(0)
	endRegion := kv.mergePath(math.MaxUint64)
	withRange := clientv3.WithRange(endRegion)
	withLimit := clientv3.WithLimit(rangeLimit)

	for {
		key := kv.mergePath(nextID)
		resp, err := kvGet(kv.client, key, withRange, withLimit)
		if err != nil {
			return errors.Trace(err)
		}
		if len(resp.Kvs) == 0 {
			return nil
		}

		for _, item := range resp.Kvs {
			key := string(item.Key)
			elems := strings.Split(key, "/merge/")
			if len(elems) != 2 {
				return errors.Errorf("invalid merge region key %v", key)
			}

			fromRegionID, err := strconv.ParseUint(elems[1], 10, 64)
			if err != nil {
				return errors.Trace(err)
			}

			intoRegionID, err := bytesToUint64(item.Value)
			if err != nil {
				return errors.Trace(err)
			}

			nextID = fromRegionID + 1
			regions.markRegionMerge(fromRegionID, intoRegionID)
		}
	}
}

func (kv *kv) loadShutdownRegions(regions *regionsInfo, rangeLimit int64) error {
	nextID := uint64(0)
	endRegion := kv.shutdownPath(math.MaxUint64)
	withRange := clientv3.WithRange(endRegion)
	withLimit := clientv3.WithLimit(rangeLimit)

	for {
		key := kv.shutdownPath(nextID)
		resp, err := kvGet(kv.client, key, withRange, withLimit)
		if err != nil {
			return errors.Trace(err)
		}
		if len(resp.Kvs) == 0 {
			return nil
		}

		for _, item := range resp.Kvs {
			key := string(item.Key)
			elems := strings.Split(key, "/shutdown/")
			if len(elems) != 2 {
				return errors.Errorf("invalid shutdown region key %v", key)
			}

			regionID, err := strconv.ParseUint(elems[1], 10, 64)
			if err != nil {
				return errors.Trace(err)
			}

			nextID = regionID + 1
			regions.markRegionShutdown(regionID)
		}
	}
}

func (kv *kv) loadProto(key string, msg proto.Message) (bool, error) {
	value, err := kv.load(key)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == nil {
		return false, nil
	}
	return true, proto.Unmarshal(value, msg)
}

func (kv *kv) saveProto(key string, msg proto.Message) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return errors.Trace(err)
	}
	return kv.save(key, string(value))
}

func (kv *kv) load(key string) ([]byte, error) {
	resp, err := kvGet(kv.client, key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n := len(resp.Kvs); n == 0 {
		return nil, nil
	} else if n > 1 {
		return nil, errors.Errorf("load more than one kvs: key %v kvs %v", key, n)
	}
	return resp.Kvs[0].Value, nil
}

func (kv *kv) save(key, value string) error {
	return kv.commit(clientv3.OpPut(key, value))
}

func (kv *kv) delete(key string) error {
	return kv.commit(clientv3.OpDelete(key))
}

func (kv *kv) commit(ops ...clientv3.Op) error {
	resp, err := kv.txn().Then(ops...).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Trace(errTxnFailed)
	}
	return nil
}

func kvGet(c *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), kvRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(c).Get(ctx, key, opts...)
	if cost := time.Since(start); cost > kvSlowRequestTime {
		log.Warnf("kv gets too slow: key %v cost %v err %v", key, cost, err)
	}

	return resp, errors.Trace(err)
}
