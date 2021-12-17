// Copyright 2017 TiKV Project Authors.
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

package core

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
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

// Storage wraps all kv operations, keep it stateless.
type Storage struct {
	kv.Base
	regionStorage        *RegionStorage
	encryptionKeyManager *encryptionkm.KeyManager
	useRegionStorage     int32
	regionLoaded         int32
	mu                   sync.Mutex
}

// StorageOpt represents available options to create Storage.
type StorageOpt struct {
	regionStorage        *RegionStorage
	encryptionKeyManager *encryptionkm.KeyManager
}

// StorageOption configures StorageOpt
type StorageOption func(*StorageOpt)

// WithRegionStorage sets RegionStorage to the Storage
func WithRegionStorage(regionStorage *RegionStorage) StorageOption {
	return func(opt *StorageOpt) {
		opt.regionStorage = regionStorage
	}
}

// WithEncryptionKeyManager sets EncryptionManager to the Storage
func WithEncryptionKeyManager(encryptionKeyManager *encryptionkm.KeyManager) StorageOption {
	return func(opt *StorageOpt) {
		opt.encryptionKeyManager = encryptionKeyManager
	}
}

// NewStorage creates Storage instance with Base.
func NewStorage(base kv.Base, opts ...StorageOption) *Storage {
	options := &StorageOpt{}
	for _, opt := range opts {
		opt(options)
	}
	return &Storage{
		Base:                 base,
		regionStorage:        options.regionStorage,
		encryptionKeyManager: options.encryptionKeyManager,
	}
}

// GetRegionStorage gets the region storage.
func (s *Storage) GetRegionStorage() *RegionStorage {
	return s.regionStorage
}

// SwitchToRegionStorage switches to the region storage.
func (s *Storage) SwitchToRegionStorage() {
	atomic.StoreInt32(&s.useRegionStorage, 1)
}

// SwitchToDefaultStorage switches to the to default storage.
func (s *Storage) SwitchToDefaultStorage() {
	atomic.StoreInt32(&s.useRegionStorage, 0)
}

func (s *Storage) storePath(storeID uint64) string {
	return path.Join(clusterPath, "s", fmt.Sprintf("%020d", storeID))
}

func regionPath(regionID uint64) string {
	return path.Join(clusterPath, "r", fmt.Sprintf("%020d", regionID))
}

// ClusterStatePath returns the path to save an option.
func (s *Storage) ClusterStatePath(option string) string {
	return path.Join(clusterPath, "status", option)
}

func (s *Storage) storeLeaderWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "leader")
}

func (s *Storage) storeRegionWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "region")
}

// EncryptionKeysPath returns the path to save encryption keys.
func (s *Storage) EncryptionKeysPath() string {
	return path.Join(encryptionKeysPath, "keys")
}

// SaveScheduleConfig saves the config of scheduler.
func (s *Storage) SaveScheduleConfig(scheduleName string, data []byte) error {
	configPath := path.Join(customScheduleConfigPath, scheduleName)
	return s.Save(configPath, string(data))
}

// RemoveScheduleConfig removes the config of scheduler.
func (s *Storage) RemoveScheduleConfig(scheduleName string) error {
	configPath := path.Join(customScheduleConfigPath, scheduleName)
	return s.Remove(configPath)
}

// LoadMeta loads cluster meta from storage.
func (s *Storage) LoadMeta(meta *metapb.Cluster) (bool, error) {
	return loadProto(s.Base, clusterPath, meta)
}

// SaveMeta save cluster meta to storage.
func (s *Storage) SaveMeta(meta *metapb.Cluster) error {
	return saveProto(s.Base, clusterPath, meta)
}

// LoadStore loads one store from storage.
func (s *Storage) LoadStore(storeID uint64, store *metapb.Store) (bool, error) {
	return loadProto(s.Base, s.storePath(storeID), store)
}

// SaveStore saves one store to storage.
func (s *Storage) SaveStore(store *metapb.Store) error {
	return saveProto(s.Base, s.storePath(store.GetId()), store)
}

// DeleteStore deletes one store from storage.
func (s *Storage) DeleteStore(store *metapb.Store) error {
	return s.Remove(s.storePath(store.GetId()))
}

// LoadRegion loads one region from storage.
func (s *Storage) LoadRegion(regionID uint64, region *metapb.Region) (ok bool, err error) {
	if atomic.LoadInt32(&s.useRegionStorage) > 0 {
		return loadRegion(s.regionStorage, s.encryptionKeyManager, regionID, region)
	}
	return loadRegion(s.Base, s.encryptionKeyManager, regionID, region)
}

// LoadRegions loads all regions from storage to RegionsInfo.
func (s *Storage) LoadRegions(ctx context.Context, f func(region *RegionInfo) []*RegionInfo) error {
	if atomic.LoadInt32(&s.useRegionStorage) > 0 {
		return loadRegions(ctx, s.regionStorage, s.encryptionKeyManager, f)
	}
	return loadRegions(ctx, s.Base, s.encryptionKeyManager, f)
}

// LoadRegionsOnce loads all regions from storage to RegionsInfo.Only load one time from regionStorage.
func (s *Storage) LoadRegionsOnce(ctx context.Context, f func(region *RegionInfo) []*RegionInfo) error {
	if atomic.LoadInt32(&s.useRegionStorage) == 0 {
		return loadRegions(ctx, s.Base, s.encryptionKeyManager, f)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.regionLoaded == 0 {
		if err := loadRegions(ctx, s.regionStorage, s.encryptionKeyManager, f); err != nil {
			return err
		}
		s.regionLoaded = 1
	}
	return nil
}

// SaveRegion saves one region to storage.
func (s *Storage) SaveRegion(region *metapb.Region) error {
	if atomic.LoadInt32(&s.useRegionStorage) > 0 {
		return s.regionStorage.SaveRegion(region)
	}
	return saveRegion(s.Base, s.encryptionKeyManager, region)
}

// DeleteRegion deletes one region from storage.
func (s *Storage) DeleteRegion(region *metapb.Region) error {
	if atomic.LoadInt32(&s.useRegionStorage) > 0 {
		return deleteRegion(s.regionStorage, region)
	}
	return deleteRegion(s.Base, region)
}

// SaveConfig stores marshallable cfg to the configPath.
func (s *Storage) SaveConfig(cfg interface{}) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	return s.Save(configPath, string(value))
}

// LoadConfig loads config from configPath then unmarshal it to cfg.
func (s *Storage) LoadConfig(cfg interface{}) (bool, error) {
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

// SaveRule stores a rule cfg to the rulesPath.
func (s *Storage) SaveRule(ruleKey string, rule interface{}) error {
	return s.saveJSON(rulesPath, ruleKey, rule)
}

// DeleteRule removes a rule from storage.
func (s *Storage) DeleteRule(ruleKey string) error {
	return s.Remove(path.Join(rulesPath, ruleKey))
}

// LoadRules loads placement rules from storage.
func (s *Storage) LoadRules(f func(k, v string)) error {
	return s.loadRangeByPrefix(rulesPath+"/", f)
}

// SaveRegionRule saves a region rule to the storage.
func (s *Storage) SaveRegionRule(ruleKey string, rule interface{}) error {
	return s.saveJSON(regionLabelPath, ruleKey, rule)
}

// DeleteRegionRule removes a region rule from storage.
func (s *Storage) DeleteRegionRule(ruleKey string) error {
	return s.Remove(path.Join(regionLabelPath, ruleKey))
}

// LoadRegionRules loads region rules from storage.
func (s *Storage) LoadRegionRules(f func(k, v string)) error {
	return s.loadRangeByPrefix(regionLabelPath+"/", f)
}

// SaveRuleGroup stores a rule group config to storage.
func (s *Storage) SaveRuleGroup(groupID string, group interface{}) error {
	return s.saveJSON(ruleGroupPath, groupID, group)
}

// DeleteRuleGroup removes a rule group from storage.
func (s *Storage) DeleteRuleGroup(groupID string) error {
	return s.Remove(path.Join(ruleGroupPath, groupID))
}

// LoadRuleGroups loads all rule groups from storage.
func (s *Storage) LoadRuleGroups(f func(k, v string)) error {
	return s.loadRangeByPrefix(ruleGroupPath+"/", f)
}

// saveJSON saves json format data to storage.
func (s *Storage) saveJSON(prefix, key string, data interface{}) error {
	value, err := json.Marshal(data)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByArgs()
	}
	return s.Save(path.Join(prefix, key), string(value))
}

// loadRangeByPrefix iterates all key-value pairs in the storage that has the prefix.
func (s *Storage) loadRangeByPrefix(prefix string, f func(k, v string)) error {
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

// SaveReplicationStatus stores replication status by mode.
func (s *Storage) SaveReplicationStatus(mode string, status interface{}) error {
	value, err := json.Marshal(status)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByArgs()
	}
	return s.Save(path.Join(replicationPath, mode), string(value))
}

// LoadReplicationStatus loads replication status by mode.
func (s *Storage) LoadReplicationStatus(mode string, status interface{}) (bool, error) {
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

// SaveComponent stores marshallable components to the componentPath.
func (s *Storage) SaveComponent(component interface{}) error {
	value, err := json.Marshal(component)
	if err != nil {
		return errors.WithStack(err)
	}
	return s.Save(componentPath, string(value))
}

// LoadComponent loads components from componentPath then unmarshal it to component.
func (s *Storage) LoadComponent(component interface{}) (bool, error) {
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

// LoadStores loads all stores from storage to StoresInfo.
func (s *Storage) LoadStores(f func(store *StoreInfo)) error {
	nextID := uint64(0)
	endKey := s.storePath(math.MaxUint64)
	for {
		key := s.storePath(nextID)
		_, res, err := s.LoadRange(key, endKey, minKVRangeLimit)
		if err != nil {
			return err
		}
		for _, str := range res {
			store := &metapb.Store{}
			if err := store.Unmarshal([]byte(str)); err != nil {
				return errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByArgs()
			}
			leaderWeight, err := s.loadFloatWithDefaultValue(s.storeLeaderWeightPath(store.GetId()), 1.0)
			if err != nil {
				return err
			}
			regionWeight, err := s.loadFloatWithDefaultValue(s.storeRegionWeightPath(store.GetId()), 1.0)
			if err != nil {
				return err
			}
			newStoreInfo := NewStoreInfo(store, SetLeaderWeight(leaderWeight), SetRegionWeight(regionWeight))

			nextID = store.GetId() + 1
			f(newStoreInfo)
		}
		if len(res) < minKVRangeLimit {
			return nil
		}
	}
}

// SaveStoreWeight saves a store's leader and region weight to storage.
func (s *Storage) SaveStoreWeight(storeID uint64, leader, region float64) error {
	leaderValue := strconv.FormatFloat(leader, 'f', -1, 64)
	if err := s.Save(s.storeLeaderWeightPath(storeID), leaderValue); err != nil {
		return err
	}
	regionValue := strconv.FormatFloat(region, 'f', -1, 64)
	return s.Save(s.storeRegionWeightPath(storeID), regionValue)
}

func (s *Storage) loadFloatWithDefaultValue(path string, def float64) (float64, error) {
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

// Flush flushes the dirty region to storage.
func (s *Storage) Flush() error {
	if s.regionStorage != nil {
		return s.regionStorage.FlushRegion()
	}
	return nil
}

// Close closes the s.
func (s *Storage) Close() error {
	if s.regionStorage != nil {
		err := s.regionStorage.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// SaveGCSafePoint saves new GC safe point to storage.
func (s *Storage) SaveGCSafePoint(safePoint uint64) error {
	key := path.Join(gcPath, "safe_point")
	value := strconv.FormatUint(safePoint, 16)
	return s.Save(key, value)
}

// LoadGCSafePoint loads current GC safe point from storage.
func (s *Storage) LoadGCSafePoint() (uint64, error) {
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

// ServiceSafePoint is the safepoint for a specific service
type ServiceSafePoint struct {
	ServiceID string `json:"service_id"`
	ExpiredAt int64  `json:"expired_at"`
	SafePoint uint64 `json:"safe_point"`
}

// SaveServiceGCSafePoint saves a GC safepoint for the service
func (s *Storage) SaveServiceGCSafePoint(ssp *ServiceSafePoint) error {
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
func (s *Storage) RemoveServiceGCSafePoint(serviceID string) error {
	if serviceID == gcWorkerServiceSafePointID {
		return errors.New("cannot remove service safe point of gc_worker")
	}
	key := path.Join(gcPath, "safe_point", "service", serviceID)
	return s.Remove(key)
}

func (s *Storage) initServiceGCSafePointForGCWorker(initialValue uint64) (*ServiceSafePoint, error) {
	ssp := &ServiceSafePoint{
		ServiceID: gcWorkerServiceSafePointID,
		SafePoint: initialValue,
		ExpiredAt: math.MaxInt64,
	}
	if err := s.SaveServiceGCSafePoint(ssp); err != nil {
		return nil, err
	}
	return ssp, nil
}

// LoadMinServiceGCSafePoint returns the minimum safepoint across all services
func (s *Storage) LoadMinServiceGCSafePoint(now time.Time) (*ServiceSafePoint, error) {
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
	min := &ServiceSafePoint{SafePoint: math.MaxUint64}
	for i, key := range keys {
		ssp := &ServiceSafePoint{}
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

// GetAllServiceGCSafePoints returns all services GC safepoints
func (s *Storage) GetAllServiceGCSafePoints() ([]*ServiceSafePoint, error) {
	prefix := path.Join(gcPath, "safe_point", "service") + "/"
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := s.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []*ServiceSafePoint{}, nil
	}

	ssps := make([]*ServiceSafePoint, 0, len(keys))
	for i := range keys {
		ssp := &ServiceSafePoint{}
		if err := json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
		}
		ssps = append(ssps, ssp)
	}

	return ssps, nil
}

// LoadAllScheduleConfig loads all schedulers' config.
func (s *Storage) LoadAllScheduleConfig() ([]string, []string, error) {
	prefix := customScheduleConfigPath + "/"
	keys, values, err := s.LoadRange(prefix, clientv3.GetPrefixRangeEnd(prefix), 1000)
	for i, key := range keys {
		keys[i] = strings.TrimPrefix(key, prefix)
	}
	return keys, values, err
}

func loadProto(s kv.Base, key string, msg proto.Message) (bool, error) {
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

func saveProto(s kv.Base, key string, msg proto.Message) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
	}
	return s.Save(key, string(value))
}

func loadRegion(
	kv kv.Base,
	encryptionKeyManager *encryptionkm.KeyManager,
	regionID uint64,
	region *metapb.Region,
) (ok bool, err error) {
	value, err := kv.Load(regionPath(regionID))
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
	err = encryption.DecryptRegion(region, encryptionKeyManager)
	return true, err
}

func saveRegion(
	kv kv.Base,
	encryptionKeyManager *encryptionkm.KeyManager,
	region *metapb.Region,
) error {
	region, err := encryption.EncryptRegion(region, encryptionKeyManager)
	if err != nil {
		return err
	}
	value, err := proto.Marshal(region)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByArgs()
	}
	return kv.Save(regionPath(region.GetId()), string(value))
}
