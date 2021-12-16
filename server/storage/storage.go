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

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/encryptionkm"
	"github.com/tikv/pd/server/kv"
	"go.etcd.io/etcd/clientv3"
)

const (
	clusterPath                = "raft"
	configPath                 = "config"
	schedulePath               = "schedule"
	gcPath                     = "gc"
	rulesPath                  = "rules"
	ruleGroupPath              = "rule_group"
	regionLabelPath            = "region_label"
	replicationPath            = "replication_mode"
	componentPath              = "component"
	customScheduleConfigPath   = "scheduler_config"
	encryptionKeysPath         = "encryption_keys"
	gcWorkerServiceSafePointID = "gc_worker"
)

const (
	maxKVRangeLimit = 10000
	minKVRangeLimit = 100
)

// Storage is the interface for the backend storage of the PD.
// TODO: replace the core.Storage with this interface later.
type Storage interface {
	ConfigStorage
	MetaStorage
	RuleStorage
	ComponentStorage
	ReplicationStatusStorage
	GCSafePointStorage
}

type defaultStorage struct {
	kv.Base
	encryptionKeyManager *encryptionkm.KeyManager
}

// ConfigStorage defines the storage operations on the config.
type ConfigStorage interface {
	LoadConfig(cfg interface{}) (bool, error)
	SaveConfig(cfg interface{}) error
	LoadAllScheduleConfig() ([]string, []string, error)
	SaveScheduleConfig(scheduleName string, data []byte) error
	RemoveScheduleConfig(scheduleName string) error
}

// LoadConfig loads config from configPath then unmarshal it to cfg.
func (s *defaultStorage) LoadConfig(cfg interface{}) (bool, error) {
	value, err := s.Load(configPath)
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
	}
	err = json.Unmarshal([]byte(value), cfg)
	if err != nil {
		return false, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return true, nil
}

// SaveConfig stores marshallable cfg to the configPath.
func (s *defaultStorage) SaveConfig(cfg interface{}) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	return s.Save(configPath, string(value))
}

// LoadAllScheduleConfig loads all schedulers' config.
func (s *defaultStorage) LoadAllScheduleConfig() ([]string, []string, error) {
	prefix := customScheduleConfigPath + "/"
	keys, values, err := s.LoadRange(prefix, clientv3.GetPrefixRangeEnd(prefix), 1000)
	for i, key := range keys {
		keys[i] = strings.TrimPrefix(key, prefix)
	}
	return keys, values, err
}

// SaveScheduleConfig saves the config of scheduler.
func (s *defaultStorage) SaveScheduleConfig(scheduleName string, data []byte) error {
	configPath := path.Join(customScheduleConfigPath, scheduleName)
	return s.Save(configPath, string(data))
}

// RemoveScheduleConfig removes the config of scheduler.
func (s *defaultStorage) RemoveScheduleConfig(scheduleName string) error {
	configPath := path.Join(customScheduleConfigPath, scheduleName)
	return s.Remove(configPath)
}

// MetaStorage defines the storage operations on the PD cluster meta info.
type MetaStorage interface {
	LoadMeta(meta *metapb.Cluster) (bool, error)
	SaveMeta(meta *metapb.Cluster) error
	LoadStore(storeID uint64, store *metapb.Store) (bool, error)
	SaveStore(store *metapb.Store) error
	SaveStoreWeight(storeID uint64, leader, region float64) error
	LoadStores(f func(store *core.StoreInfo)) error
	DeleteStore(store *metapb.Store) error
	LoadRegion(regionID uint64, region *metapb.Region) (ok bool, err error)
	LoadRegions(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error
	SaveRegion(region *metapb.Region) error
	DeleteRegion(region *metapb.Region) error
}

// LoadMeta loads cluster meta from the storage. This method will only
// be used by the PD server, so we should only implement it for the etcd storage.
func (s *defaultStorage) LoadMeta(meta *metapb.Cluster) (bool, error) {
	return s.loadProto(clusterPath, meta)
}

func (s *defaultStorage) loadProto(key string, msg proto.Message) (bool, error) {
	value, err := s.Load(key)
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
	}
	err = proto.Unmarshal([]byte(value), msg)
	if err != nil {
		return false, errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return true, nil
}

// SaveMeta save cluster meta to the storage. This method will only
// be used by the PD server, so we should only implement it for the etcd storage.
func (s *defaultStorage) SaveMeta(meta *metapb.Cluster) error {
	return s.saveProto(clusterPath, meta)
}

func (s *defaultStorage) saveProto(key string, msg proto.Message) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
	}
	return s.Save(key, string(value))
}

// LoadStore loads one store from storage.
func (s *defaultStorage) LoadStore(storeID uint64, store *metapb.Store) (bool, error) {
	return s.loadProto(storePath(storeID), store)
}

// SaveStore saves one store to storage.
func (s *defaultStorage) SaveStore(store *metapb.Store) error {
	return s.saveProto(storePath(store.GetId()), store)
}

// SaveStoreWeight saves a store's leader and region weight to storage.
func (s *defaultStorage) SaveStoreWeight(storeID uint64, leader, region float64) error {
	leaderValue := strconv.FormatFloat(leader, 'f', -1, 64)
	if err := s.Save(storeLeaderWeightPath(storeID), leaderValue); err != nil {
		return err
	}
	regionValue := strconv.FormatFloat(region, 'f', -1, 64)
	return s.Save(storeRegionWeightPath(storeID), regionValue)
}

// LoadStores loads all stores from storage to StoresInfo.
func (s *defaultStorage) LoadStores(f func(store *core.StoreInfo)) error {
	nextID := uint64(0)
	endKey := storePath(math.MaxUint64)
	for {
		key := storePath(nextID)
		_, res, err := s.LoadRange(key, endKey, minKVRangeLimit)
		if err != nil {
			return err
		}
		for _, str := range res {
			store := &metapb.Store{}
			if err := store.Unmarshal([]byte(str)); err != nil {
				return errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByArgs()
			}
			leaderWeight, err := s.loadFloatWithDefaultValue(storeLeaderWeightPath(store.GetId()), 1.0)
			if err != nil {
				return err
			}
			regionWeight, err := s.loadFloatWithDefaultValue(storeRegionWeightPath(store.GetId()), 1.0)
			if err != nil {
				return err
			}
			newStoreInfo := core.NewStoreInfo(store, core.SetLeaderWeight(leaderWeight), core.SetRegionWeight(regionWeight))

			nextID = store.GetId() + 1
			f(newStoreInfo)
		}
		if len(res) < minKVRangeLimit {
			return nil
		}
	}
}

func (s *defaultStorage) loadFloatWithDefaultValue(path string, def float64) (float64, error) {
	res, err := s.Load(path)
	if err != nil {
		return 0, err
	}
	if res == "" {
		return def, nil
	}
	val, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return 0, errs.ErrStrconvParseFloat.Wrap(err).GenWithStackByArgs()
	}
	return val, nil
}

// DeleteStore deletes one store from storage.
func (s *defaultStorage) DeleteStore(store *metapb.Store) error {
	return s.Remove(storePath(store.GetId()))
}

// LoadRegion loads one region from the backend storage.
func (s *defaultStorage) LoadRegion(regionID uint64, region *metapb.Region) (ok bool, err error) {
	value, err := s.Load(regionPath(regionID))
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
	}
	err = proto.Unmarshal([]byte(value), region)
	if err != nil {
		return true, errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByArgs()
	}
	err = encryption.DecryptRegion(region, s.encryptionKeyManager)
	return true, err
}

// LoadRegions loads all regions from storage to RegionsInfo.
func (s *defaultStorage) LoadRegions(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error {
	nextID := uint64(0)
	endKey := regionPath(math.MaxUint64)

	// Since the region key may be very long, using a larger rangeLimit will cause
	// the message packet to exceed the grpc message size limit (4MB). Here we use
	// a variable rangeLimit to work around.
	rangeLimit := maxKVRangeLimit
	for {
		failpoint.Inject("slowLoadRegion", func() {
			rangeLimit = 1
			time.Sleep(time.Second)
		})
		startKey := regionPath(nextID)
		_, res, err := s.LoadRange(startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= minKVRangeLimit {
				continue
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		for _, r := range res {
			region := &metapb.Region{}
			if err := region.Unmarshal([]byte(r)); err != nil {
				return errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByArgs()
			}
			if err = encryption.DecryptRegion(region, s.encryptionKeyManager); err != nil {
				return err
			}

			nextID = region.GetId() + 1
			overlaps := f(core.NewRegionInfo(region, nil))
			for _, item := range overlaps {
				if err := s.DeleteRegion(item.GetMeta()); err != nil {
					return err
				}
			}
		}

		if len(res) < rangeLimit {
			return nil
		}
	}
}

// SaveRegion saves one region to storage.
func (s *defaultStorage) SaveRegion(region *metapb.Region) error {
	region, err := encryption.EncryptRegion(region, s.encryptionKeyManager)
	if err != nil {
		return err
	}
	value, err := proto.Marshal(region)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByArgs()
	}
	return s.Save(regionPath(region.GetId()), string(value))
}

// DeleteRegion deletes one region from storage.
func (s *defaultStorage) DeleteRegion(region *metapb.Region) error {
	return s.Remove(regionPath(region.GetId()))
}

// RuleStorage defines the storage operations on the rule.
type RuleStorage interface {
	LoadRules(f func(k, v string)) error
	SaveRule(ruleKey string, rule interface{}) error
	DeleteRule(ruleKey string) error
	LoadRuleGroups(f func(k, v string)) error
	SaveRuleGroup(groupID string, group interface{}) error
	DeleteRuleGroup(groupID string) error
	LoadRegionRules(f func(k, v string)) error
	SaveRegionRule(ruleKey string, rule interface{}) error
	DeleteRegionRule(ruleKey string) error
}

// LoadRules loads placement rules from storage.
func (s *defaultStorage) LoadRules(f func(k, v string)) error {
	return s.loadRangeByPrefix(rulesPath+"/", f)
}

// loadRangeByPrefix iterates all key-value pairs in the storage that has the prefix.
func (s *defaultStorage) loadRangeByPrefix(prefix string, f func(k, v string)) error {
	nextKey := prefix
	endKey := clientv3.GetPrefixRangeEnd(prefix)
	for {
		keys, values, err := s.LoadRange(nextKey, endKey, minKVRangeLimit)
		if err != nil {
			return err
		}
		for i := range keys {
			f(strings.TrimPrefix(keys[i], prefix), values[i])
		}
		if len(keys) < minKVRangeLimit {
			return nil
		}
		nextKey = keys[len(keys)-1] + "\x00"
	}
}

// SaveRule stores a rule cfg to the rulesPath.
func (s *defaultStorage) SaveRule(ruleKey string, rule interface{}) error {
	return s.saveJSON(rulesPath, ruleKey, rule)
}

// DeleteRule removes a rule from storage.
func (s *defaultStorage) DeleteRule(ruleKey string) error {
	return s.Remove(path.Join(rulesPath, ruleKey))
}

func (s *defaultStorage) saveJSON(prefix, key string, data interface{}) error {
	value, err := json.Marshal(data)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByArgs()
	}
	return s.Save(path.Join(prefix, key), string(value))
}

// LoadRuleGroups loads all rule groups from storage.
func (s *defaultStorage) LoadRuleGroups(f func(k, v string)) error {
	return s.loadRangeByPrefix(ruleGroupPath+"/", f)
}

// SaveRuleGroup stores a rule group config to storage.
func (s *defaultStorage) SaveRuleGroup(groupID string, group interface{}) error {
	return s.saveJSON(ruleGroupPath, groupID, group)
}

// DeleteRuleGroup removes a rule group from storage.
func (s *defaultStorage) DeleteRuleGroup(groupID string) error {
	return s.Remove(path.Join(ruleGroupPath, groupID))
}

// LoadRegionRules loads region rules from storage.
func (s *defaultStorage) LoadRegionRules(f func(k, v string)) error {
	return s.loadRangeByPrefix(regionLabelPath+"/", f)
}

// SaveRegionRule saves a region rule to the storage.
func (s *defaultStorage) SaveRegionRule(ruleKey string, rule interface{}) error {
	return s.saveJSON(regionLabelPath, ruleKey, rule)
}

// DeleteRegionRule removes a region rule from storage.
func (s *defaultStorage) DeleteRegionRule(ruleKey string) error {
	return s.Remove(path.Join(regionLabelPath, ruleKey))
}

// ComponentStorage defines the storage operations on the component.
type ComponentStorage interface {
	LoadComponent(component interface{}) (bool, error)
	SaveComponent(component interface{}) error
}

// LoadComponent loads components from componentPath then unmarshal it to component.
func (s *defaultStorage) LoadComponent(component interface{}) (bool, error) {
	v, err := s.Load(componentPath)
	if err != nil {
		return false, err
	}
	if v == "" {
		return false, nil
	}
	err = json.Unmarshal([]byte(v), component)
	if err != nil {
		return false, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByArgs()
	}
	return true, nil
}

// SaveComponent stores marshallable components to the componentPath.
func (s *defaultStorage) SaveComponent(component interface{}) error {
	value, err := json.Marshal(component)
	if err != nil {
		return errors.WithStack(err)
	}
	return s.Save(componentPath, string(value))
}

// ReplicationStatusStorage defines the storage operations on the replication status.
type ReplicationStatusStorage interface {
	LoadReplicationStatus(mode string, status interface{}) (bool, error)
	SaveReplicationStatus(mode string, status interface{}) error
}

// LoadReplicationStatus loads replication status by mode.
func (s *defaultStorage) LoadReplicationStatus(mode string, status interface{}) (bool, error) {
	v, err := s.Load(path.Join(replicationPath, mode))
	if err != nil {
		return false, err
	}
	if v == "" {
		return false, nil
	}
	err = json.Unmarshal([]byte(v), status)
	if err != nil {
		return false, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByArgs()
	}
	return true, nil
}

// SaveReplicationStatus stores replication status by mode.
func (s *defaultStorage) SaveReplicationStatus(mode string, status interface{}) error {
	value, err := json.Marshal(status)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByArgs()
	}
	return s.Save(path.Join(replicationPath, mode), string(value))
}

// GCSafePointStorage defines the storage operations on the GC safe point.
type GCSafePointStorage interface {
	LoadGCSafePoint() (uint64, error)
	SaveGCSafePoint(safePoint uint64) error
	LoadMinServiceGCSafePoint(now time.Time) (*core.ServiceSafePoint, error)
	LoadAllServiceGCSafePoints() ([]*core.ServiceSafePoint, error)
	SaveServiceGCSafePoint(ssp *core.ServiceSafePoint) error
	RemoveServiceGCSafePoint(serviceID string) error
}

// LoadGCSafePoint loads current GC safe point from storage.
func (s *defaultStorage) LoadGCSafePoint() (uint64, error) {
	key := path.Join(gcPath, "safe_point")
	value, err := s.Load(key)
	if err != nil {
		return 0, err
	}
	if value == "" {
		return 0, nil
	}
	safePoint, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return 0, err
	}
	return safePoint, nil
}

// SaveGCSafePoint saves new GC safe point to storage.
func (s *defaultStorage) SaveGCSafePoint(safePoint uint64) error {
	key := path.Join(gcPath, "safe_point")
	value := strconv.FormatUint(safePoint, 16)
	return s.Save(key, value)
}

// LoadMinServiceGCSafePoint returns the minimum safepoint across all services
func (s *defaultStorage) LoadMinServiceGCSafePoint(now time.Time) (*core.ServiceSafePoint, error) {
	prefix := path.Join(gcPath, "safe_point", "service") + "/"
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := s.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		// There's no service safepoint. It may be a new cluster, or upgraded from an older version where all service
		// safepoints are missing. For the second case, we have no way to recover it. Store an initial value 0 for
		// gc_worker.
		return s.initServiceGCSafePointForGCWorker(0)
	}

	hasGCWorker := false
	min := &core.ServiceSafePoint{SafePoint: math.MaxUint64}
	for i, key := range keys {
		ssp := &core.ServiceSafePoint{}
		if err := json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
		}
		if ssp.ServiceID == gcWorkerServiceSafePointID {
			hasGCWorker = true
			// If gc_worker's expire time is incorrectly set, fix it.
			if ssp.ExpiredAt != math.MaxInt64 {
				ssp.ExpiredAt = math.MaxInt64
				err = s.SaveServiceGCSafePoint(ssp)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}

		if ssp.ExpiredAt < now.Unix() {
			s.Remove(key)
			continue
		}
		if ssp.SafePoint < min.SafePoint {
			min = ssp
		}
	}

	if min.SafePoint == math.MaxUint64 {
		// There's no valid safepoints and we have no way to recover it. Just set gc_worker to 0.
		log.Info("there are no valid service safepoints. init gc_worker's service safepoint to 0")
		return s.initServiceGCSafePointForGCWorker(0)
	}

	if !hasGCWorker {
		// If there exists some service safepoints but gc_worker is missing, init it with the min value among all
		// safepoints (including expired ones)
		return s.initServiceGCSafePointForGCWorker(min.SafePoint)
	}

	return min, nil
}

func (s *defaultStorage) initServiceGCSafePointForGCWorker(initialValue uint64) (*core.ServiceSafePoint, error) {
	ssp := &core.ServiceSafePoint{
		ServiceID: gcWorkerServiceSafePointID,
		SafePoint: initialValue,
		ExpiredAt: math.MaxInt64,
	}
	if err := s.SaveServiceGCSafePoint(ssp); err != nil {
		return nil, err
	}
	return ssp, nil
}

// LoadAllServiceGCSafePoints returns all services GC safepoints
func (s *defaultStorage) LoadAllServiceGCSafePoints() ([]*core.ServiceSafePoint, error) {
	prefix := path.Join(gcPath, "safe_point", "service") + "/"
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := s.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []*core.ServiceSafePoint{}, nil
	}

	ssps := make([]*core.ServiceSafePoint, 0, len(keys))
	for i := range keys {
		ssp := &core.ServiceSafePoint{}
		if err := json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
		}
		ssps = append(ssps, ssp)
	}

	return ssps, nil
}

// SaveServiceGCSafePoint saves a GC safepoint for the service
func (s *defaultStorage) SaveServiceGCSafePoint(ssp *core.ServiceSafePoint) error {
	if ssp.ServiceID == "" {
		return errors.New("service id of service safepoint cannot be empty")
	}

	if ssp.ServiceID == gcWorkerServiceSafePointID && ssp.ExpiredAt != math.MaxInt64 {
		return errors.New("TTL of gc_worker's service safe point must be infinity")
	}

	key := path.Join(gcPath, "safe_point", "service", ssp.ServiceID)
	value, err := json.Marshal(ssp)
	if err != nil {
		return err
	}

	return s.Save(key, string(value))
}

// RemoveServiceGCSafePoint removes a GC safepoint for the service
func (s *defaultStorage) RemoveServiceGCSafePoint(serviceID string) error {
	if serviceID == gcWorkerServiceSafePointID {
		return errors.New("cannot remove service safe point of gc_worker")
	}
	key := path.Join(gcPath, "safe_point", "service", serviceID)
	return s.Remove(key)
}

func regionPath(regionID uint64) string {
	return path.Join(clusterPath, "r", fmt.Sprintf("%020d", regionID))
}
