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
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"golang.org/x/net/context"
)

const (
	kvRangeLimit      = 10000
	kvRequestTimeout  = time.Second * 10
	kvSlowRequestTime = time.Second * 1
)

var (
	errTxnFailed = errors.New("failed to commit transaction")
)

// kv wraps all kv operations, keep it stateless.
type kv struct {
	s            *Server
	client       *clientv3.Client
	clusterPath  string
	configPath   string
	schedulePath string
}

func newKV(s *Server) *kv {
	return &kv{
		s:            s,
		client:       s.client,
		clusterPath:  path.Join(s.rootPath, "raft"),
		configPath:   path.Join(s.rootPath, "config"),
		schedulePath: path.Join(s.rootPath, "schedule"),
	}
}

func (kv *kv) txn(cs ...clientv3.Cmp) clientv3.Txn { return kv.s.leaderTxn(cs...) }

func (kv *kv) storePath(storeID uint64) string {
	return path.Join(kv.clusterPath, "s", fmt.Sprintf("%020d", storeID))
}

func (kv *kv) regionPath(regionID uint64) string {
	return path.Join(kv.clusterPath, "r", fmt.Sprintf("%020d", regionID))
}

func (kv *kv) clusterStatePath(option string) string {
	return path.Join(kv.clusterPath, "status", option)
}

func (kv *kv) storeLeaderWeightPath(storeID uint64) string {
	return path.Join(kv.schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "leader")
}

func (kv *kv) storeRegionWeightPath(storeID uint64) string {
	return path.Join(kv.schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "region")
}

func (kv *kv) getRaftClusterBootstrapTime() (time.Time, error) {
	data, err := kv.load(kv.clusterStatePath("raft_bootstrap_time"))
	if err != nil {
		return zeroTime, errors.Trace(err)
	}
	if len(data) == 0 {
		return zeroTime, nil
	}
	return parseTimestamp(data)
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

func (kv *kv) loadScheduleOption(opt *scheduleOption) (bool, error) {
	cfg := &Config{}
	cfg.Schedule = *opt.load()
	cfg.Replication = *opt.rep.load()
	isExist, err := kv.loadConfig(cfg)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !isExist {
		return false, nil
	}
	opt.store(&cfg.Schedule)
	opt.rep.store(&cfg.Replication)
	return true, nil
}

func (kv *kv) saveScheduleOption(opt *scheduleOption) error {
	cfg := &Config{}
	cfg.Schedule = *opt.load()
	cfg.Replication = *opt.rep.load()
	return kv.saveConfig(cfg)
}

func (kv *kv) saveConfig(cfg *Config) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return errors.Trace(err)
	}
	return kv.save(kv.configPath, string(value))
}

func (kv *kv) loadConfig(cfg *Config) (bool, error) {
	value, err := kv.load(kv.configPath)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == nil {
		return false, nil
	}
	err = json.Unmarshal(value, cfg)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (kv *kv) loadStores(stores *core.StoresInfo, rangeLimit int64) error {
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

			storeInfo := core.NewStoreInfo(store)
			leaderWeight, err := kv.loadFloatWithDefaultValue(kv.storeLeaderWeightPath(storeInfo.GetId()), 1.0)
			if err != nil {
				return errors.Trace(err)
			}
			storeInfo.LeaderWeight = leaderWeight
			regionWeight, err := kv.loadFloatWithDefaultValue(kv.storeRegionWeightPath(storeInfo.GetId()), 1.0)
			if err != nil {
				return errors.Trace(err)
			}
			storeInfo.RegionWeight = regionWeight

			nextID = store.GetId() + 1
			stores.SetStore(storeInfo)
		}

		if len(resp.Kvs) < int(rangeLimit) {
			return nil
		}
	}
}

func (kv *kv) saveStoreWeight(storeID uint64, leader, region float64) error {
	leaderValue := strconv.FormatFloat(leader, 'f', -1, 64)
	if err := kv.save(kv.storeLeaderWeightPath(storeID), leaderValue); err != nil {
		return errors.Trace(err)
	}
	regionValue := strconv.FormatFloat(region, 'f', -1, 64)
	if err := kv.save(kv.storeRegionWeightPath(storeID), regionValue); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (kv *kv) loadFloatWithDefaultValue(path string, def float64) (float64, error) {
	res, err := kvGet(kv.client, path)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(res.Kvs) == 0 {
		return def, nil
	}
	val, err := strconv.ParseFloat(string(res.Kvs[0].Value), 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

func (kv *kv) loadRegions(regions *core.RegionsInfo, rangeLimit int64) error {
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
			regions.SetRegion(core.NewRegionInfo(region, nil))
		}

		if len(resp.Kvs) < int(rangeLimit) {
			return nil
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
	resp, err := kv.txn().Then(clientv3.OpPut(key, value)).Commit()
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
