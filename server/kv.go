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

const (
	clusterPath  = "raft"
	configPath   = "config"
	schedulePath = "schedule"
)

// kv wraps all kv operations, keep it stateless.
type kv struct {
	core.KVBase
}

func newKV(base core.KVBase) *kv {
	return &kv{
		KVBase: base,
	}
}

func (kv *kv) storePath(storeID uint64) string {
	return path.Join(clusterPath, "s", fmt.Sprintf("%020d", storeID))
}

func (kv *kv) regionPath(regionID uint64) string {
	return path.Join(clusterPath, "r", fmt.Sprintf("%020d", regionID))
}

func (kv *kv) clusterStatePath(option string) string {
	return path.Join(clusterPath, "status", option)
}

func (kv *kv) storeLeaderWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "leader")
}

func (kv *kv) storeRegionWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "region")
}

func (kv *kv) getRaftClusterBootstrapTime() (time.Time, error) {
	data, err := kv.Load(kv.clusterStatePath("raft_bootstrap_time"))
	if err != nil {
		return zeroTime, errors.Trace(err)
	}
	if len(data) == 0 {
		return zeroTime, nil
	}
	return parseTimestamp([]byte(data))
}

func (kv *kv) loadMeta(meta *metapb.Cluster) (bool, error) {
	return kv.loadProto(clusterPath, meta)
}

func (kv *kv) saveMeta(meta *metapb.Cluster) error {
	return kv.saveProto(clusterPath, meta)
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

func (kv *kv) loadScheduleOption(opt *scheduleOption) error {
	cfg := &Config{}
	cfg.Schedule = *opt.load()
	cfg.Replication = *opt.rep.load()
	isExist, err := kv.loadConfig(cfg)
	if err != nil {
		return errors.Trace(err)
	}
	if isExist {
		opt.store(&cfg.Schedule)
		opt.rep.store(&cfg.Replication)
	}

	return nil
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
	return kv.Save(configPath, string(value))
}

func (kv *kv) loadConfig(cfg *Config) (bool, error) {
	value, err := kv.Load(configPath)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == "" {
		return false, nil
	}
	err = json.Unmarshal([]byte(value), cfg)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (kv *kv) loadStores(stores *core.StoresInfo, rangeLimit int) error {
	nextID := uint64(0)
	endKey := kv.storePath(math.MaxUint64)
	for {
		key := kv.storePath(nextID)
		res, err := kv.LoadRange(key, endKey, rangeLimit)
		if err != nil {
			return errors.Trace(err)
		}
		for _, s := range res {
			store := &metapb.Store{}
			if err := store.Unmarshal([]byte(s)); err != nil {
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
		if len(res) < rangeLimit {
			return nil
		}
	}
}

func (kv *kv) saveStoreWeight(storeID uint64, leader, region float64) error {
	leaderValue := strconv.FormatFloat(leader, 'f', -1, 64)
	if err := kv.Save(kv.storeLeaderWeightPath(storeID), leaderValue); err != nil {
		return errors.Trace(err)
	}
	regionValue := strconv.FormatFloat(region, 'f', -1, 64)
	if err := kv.Save(kv.storeRegionWeightPath(storeID), regionValue); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (kv *kv) loadFloatWithDefaultValue(path string, def float64) (float64, error) {
	res, err := kv.Load(path)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if res == "" {
		return def, nil
	}
	val, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

func (kv *kv) loadRegions(regions *core.RegionsInfo, rangeLimit int) error {
	nextID := uint64(0)
	endKey := kv.regionPath(math.MaxUint64)

	for {
		key := kv.regionPath(nextID)
		res, err := kv.LoadRange(key, endKey, rangeLimit)
		if err != nil {
			return errors.Trace(err)
		}

		for _, s := range res {
			region := &metapb.Region{}
			if err := region.Unmarshal([]byte(s)); err != nil {
				return errors.Trace(err)
			}

			nextID = region.GetId() + 1
			regions.SetRegion(core.NewRegionInfo(region, nil))
		}

		if len(res) < int(rangeLimit) {
			return nil
		}
	}
}

func (kv *kv) loadProto(key string, msg proto.Message) (bool, error) {
	value, err := kv.Load(key)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == "" {
		return false, nil
	}
	return true, proto.Unmarshal([]byte(value), msg)
}

func (kv *kv) saveProto(key string, msg proto.Message) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return errors.Trace(err)
	}
	return kv.Save(key, string(value))
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

type etcdKVBase struct {
	server   *Server
	client   *clientv3.Client
	rootPath string
}

func newEtcdKVBase(s *Server) *etcdKVBase {
	return &etcdKVBase{
		server:   s,
		client:   s.client,
		rootPath: s.rootPath,
	}
}

func (kv *etcdKVBase) Load(key string) (string, error) {
	key = path.Join(kv.rootPath, key)

	resp, err := kvGet(kv.server.client, key)
	if err != nil {
		return "", errors.Trace(err)
	}
	if n := len(resp.Kvs); n == 0 {
		return "", nil
	} else if n > 1 {
		return "", errors.Errorf("load more than one kvs: key %v kvs %v", key, n)
	}
	return string(resp.Kvs[0].Value), nil
}

func (kv *etcdKVBase) LoadRange(key, endKey string, limit int) ([]string, error) {
	key = path.Join(kv.rootPath, key)
	endKey = path.Join(kv.rootPath, endKey)

	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(int64(limit))
	resp, err := kvGet(kv.server.client, key, withRange, withLimit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	res := make([]string, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		res = append(res, string(item.Value))
	}
	return res, nil
}

func (kv *etcdKVBase) Save(key, value string) error {
	key = path.Join(kv.rootPath, key)

	resp, err := kv.server.leaderTxn().Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Trace(errTxnFailed)
	}
	return nil
}
