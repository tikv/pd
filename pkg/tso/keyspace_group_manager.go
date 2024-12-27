// Copyright 2023 TiKV Project Authors.
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

package tso

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/memberutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

const (
	// mergingCheckInterval is the interval for merging check to see if the keyspace groups
	// merging process could be moved forward.
	mergingCheckInterval = 5 * time.Second
	// defaultPrimaryPriorityCheckInterval is the default interval for checking if the priorities
	// of the primaries on this TSO server/pod have changed. A goroutine will periodically check
	// do this check and re-distribute the primaries if necessary.
	defaultPrimaryPriorityCheckInterval = 10 * time.Second
	groupPatrolInterval                 = time.Minute
)

type state struct {
	syncutil.RWMutex
	// ams stores the allocator managers of the keyspace groups. Each keyspace group is
	// assigned with an allocator manager managing its global tso allocators.
	// Use a fixed size array to maximize the efficiency of concurrent access to
	// different keyspace groups for tso service.
	ams [constant.MaxKeyspaceGroupCountInUse]*AllocatorManager
	// kgs stores the keyspace groups' membership/distribution meta.
	kgs [constant.MaxKeyspaceGroupCountInUse]*endpoint.KeyspaceGroup
	// keyspaceLookupTable is a map from keyspace to the keyspace group to which it belongs.
	keyspaceLookupTable map[uint32]uint32
	// splittingGroups is the cache of splitting keyspace group related information.
	// The key is the keyspace group ID, and the value is the time when the keyspace group
	// is created as the split target. Once the split is finished, the keyspace group will
	// be removed from this map.
	splittingGroups map[uint32]time.Time
	// deletedGroups is the cache of deleted keyspace group related information.
	// Being merged will cause the group to be added to this map and finally be deleted after the merge.
	deletedGroups map[uint32]struct{}
	// requestedGroups is the cache of requested keyspace group related information.
	// Once a group receives its first TSO request and pass the certain check, it will be added to this map.
	// Being merged will cause the group to be removed from this map eventually if the merge is successful.
	requestedGroups map[uint32]struct{}
}

func (s *state) initialize() {
	s.keyspaceLookupTable = make(map[uint32]uint32)
	s.splittingGroups = make(map[uint32]time.Time)
	s.deletedGroups = make(map[uint32]struct{})
	s.requestedGroups = make(map[uint32]struct{})
}

func (s *state) deInitialize() {
	log.Info("closing all keyspace groups")

	s.Lock()
	defer s.Unlock()

	wg := sync.WaitGroup{}
	for _, am := range s.ams {
		if am != nil {
			wg.Add(1)
			go func(am *AllocatorManager) {
				defer logutil.LogPanic()
				defer wg.Done()
				am.close()
				log.Info("keyspace group closed", zap.Uint32("keyspace-group-id", am.kgID))
			}(am)
		}
	}
	wg.Wait()

	log.Info("all keyspace groups closed")
}

// getKeyspaceGroupMeta returns the meta of the given keyspace group
func (s *state) getKeyspaceGroupMeta(
	groupID uint32,
) (*AllocatorManager, *endpoint.KeyspaceGroup) {
	s.RLock()
	defer s.RUnlock()
	return s.ams[groupID], s.kgs[groupID]
}

// getSplittingGroups returns the IDs of the splitting keyspace groups.
func (s *state) getSplittingGroups() []uint32 {
	s.RLock()
	defer s.RUnlock()
	groups := make([]uint32, 0, len(s.splittingGroups))
	for groupID := range s.splittingGroups {
		groups = append(groups, groupID)
	}
	return groups
}

// getDeletedGroups returns the IDs of the deleted keyspace groups.
func (s *state) getDeletedGroups() []uint32 {
	s.RLock()
	defer s.RUnlock()
	groups := make([]uint32, 0, len(s.deletedGroups))
	for groupID := range s.deletedGroups {
		groups = append(groups, groupID)
	}
	return groups
}

// getDeletedGroupNum returns the number of the deleted keyspace groups.
func (s *state) getDeletedGroupNum() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.deletedGroups)
}

// cleanKeyspaceGroup cleans the given keyspace group from the state.
// NOTICE: currently the only legal way to delete a keyspace group is
// to merge it into another one. This function is used to clean up the
// remaining info after the merge has been finished.
func (s *state) cleanKeyspaceGroup(groupID uint32) {
	s.Lock()
	defer s.Unlock()
	delete(s.deletedGroups, groupID)
	delete(s.requestedGroups, groupID)
}

// markGroupRequested checks if the given keyspace group has been requested and should be marked.
// If yes, it will do nothing and return nil directly.
// If not, it will try to mark the keyspace group as requested inside a critical section, which
// will call the checker passed in to check if the keyspace group is qualified to be marked as requested.
// Any error encountered during the check will be returned to the caller.
func (s *state) markGroupRequested(groupID uint32, checker func() error) error {
	// Fast path to check if the keyspace group has been marked as requested.
	s.RLock()
	_, ok := s.requestedGroups[groupID]
	s.RUnlock()
	if ok {
		return nil
	}
	s.Lock()
	defer s.Unlock()
	// Double check if the keyspace group has been marked as requested.
	if _, ok := s.requestedGroups[groupID]; ok {
		return nil
	}
	if err := checker(); err != nil {
		return err
	}
	s.requestedGroups[groupID] = struct{}{}
	return nil
}

func (s *state) checkGroupSplit(
	targetGroupID uint32,
) (splitTargetAM, splitSourceAM *AllocatorManager, err error) {
	s.RLock()
	defer s.RUnlock()
	splitTargetAM, splitTargetGroup := s.ams[targetGroupID], s.kgs[targetGroupID]
	// Only the split target keyspace group needs to check the TSO split.
	if !splitTargetGroup.IsSplitTarget() {
		return nil, nil, nil // it isn't in the split state
	}
	sourceGroupID := splitTargetGroup.SplitSource()
	splitSourceAM, splitSourceGroup := s.ams[sourceGroupID], s.kgs[sourceGroupID]
	if splitSourceAM == nil || splitSourceGroup == nil {
		log.Error("the split source keyspace group is not initialized",
			zap.Uint32("source", sourceGroupID))
		return nil, nil, errs.ErrKeyspaceGroupNotInitialized.FastGenByArgs(sourceGroupID)
	}
	return splitTargetAM, splitSourceAM, nil
}

// Reject any request if the keyspace group is in merging state,
// we need to wait for the merging checker to finish the TSO merging.
func (s *state) checkGroupMerge(
	groupID uint32,
) error {
	s.RLock()
	defer s.RUnlock()
	if s.kgs[groupID] == nil || !s.kgs[groupID].IsMerging() {
		return nil
	}
	return errs.ErrKeyspaceGroupIsMerging.FastGenByArgs(groupID)
}

// getKeyspaceGroupMetaWithCheck returns the keyspace group meta of the given keyspace.
// It also checks if the keyspace is served by the given keyspace group. If not, it returns the meta
// of the keyspace group to which the keyspace currently belongs and returns NotServed (by the given
// keyspace group) error. If the keyspace doesn't belong to any keyspace group, it returns the
// NotAssigned error, which could happen because loading keyspace group meta isn't atomic when there is
// keyspace movement between keyspace groups.
func (s *state) getKeyspaceGroupMetaWithCheck(
	keyspaceID, keyspaceGroupID uint32,
) (*AllocatorManager, *endpoint.KeyspaceGroup, uint32, error) {
	s.RLock()
	defer s.RUnlock()

	if am := s.ams[keyspaceGroupID]; am != nil {
		kg := s.kgs[keyspaceGroupID]
		if kg != nil {
			if _, ok := kg.KeyspaceLookupTable[keyspaceID]; ok {
				return am, kg, keyspaceGroupID, nil
			}
		}
	}

	// The keyspace doesn't belong to this keyspace group, we should check if it belongs to any other
	// keyspace groups, and return the correct keyspace group meta to the client.
	if kgid, ok := s.keyspaceLookupTable[keyspaceID]; ok {
		if s.ams[kgid] != nil {
			return s.ams[kgid], s.kgs[kgid], kgid, nil
		}
		return nil, s.kgs[kgid], kgid, genNotServedErr(errs.ErrGetAllocatorManager, keyspaceGroupID)
	}

	// The keyspace doesn't belong to any keyspace group but the keyspace has been assigned to a
	// keyspace group before, which means the keyspace group hasn't initialized yet.
	if keyspaceGroupID != constant.DefaultKeyspaceGroupID {
		return nil, nil, keyspaceGroupID, errs.ErrKeyspaceNotAssigned.FastGenByArgs(keyspaceID)
	}

	// For migrating the existing keyspaces which have no keyspace group assigned as configured
	// in the keyspace meta. All these keyspaces will be served by the default keyspace group.
	if s.ams[constant.DefaultKeyspaceGroupID] == nil {
		return nil, nil, constant.DefaultKeyspaceGroupID,
			errs.ErrKeyspaceNotAssigned.FastGenByArgs(keyspaceID)
	}
	return s.ams[constant.DefaultKeyspaceGroupID],
		s.kgs[constant.DefaultKeyspaceGroupID],
		constant.DefaultKeyspaceGroupID, nil
}

func (s *state) getNextPrimaryToReset(
	groupID int, localAddress string,
) (member ElectionMember, kg *endpoint.KeyspaceGroup, localPriority, nextGroupID int) {
	s.RLock()
	defer s.RUnlock()

	// Both s.ams and s.kgs are arrays with the fixed size defined by the const value MaxKeyspaceGroupCountInUse.
	groupSize := int(constant.MaxKeyspaceGroupCountInUse)
	groupID %= groupSize
	for j := 0; j < groupSize; groupID, j = (groupID+1)%groupSize, j+1 {
		am := s.ams[groupID]
		kg := s.kgs[groupID]
		if am != nil && kg != nil && am.GetMember().IsLeader() {
			maxPriority := math.MinInt32
			localPriority := math.MaxInt32
			for _, member := range kg.Members {
				if member.Priority > maxPriority {
					maxPriority = member.Priority
				}
				if member.IsAddressEquivalent(localAddress) {
					localPriority = member.Priority
				}
			}

			if localPriority < maxPriority {
				// return here and reset the primary outside of the critical section
				// as resetting the primary may take some time.
				return am.GetMember(), kg, localPriority, (groupID + 1) % groupSize
			}
		}
	}

	return nil, nil, 0, groupID
}

// KeyspaceGroupManager manages the members of the keyspace groups assigned to this host.
// The replicas campaign for the leaders which provide the tso service for the corresponding
// keyspace groups.
type KeyspaceGroupManager struct {
	// state is the in-memory state of the keyspace groups
	state

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// tsoServiceID is the service ID of the TSO service, registered in the service discovery
	tsoServiceID *discovery.ServiceRegistryEntry
	etcdClient   *clientv3.Client
	httpClient   *http.Client
	// electionNamePrefix is the name prefix to generate the unique name of a participant,
	// which participate in the election of its keyspace group's primary, in the format of
	// "electionNamePrefix:keyspace-group-id"
	electionNamePrefix string
	// tsoServiceKey is the path for storing the registered tso servers.
	// Key: /ms/{cluster_id}/tso/registry/{tsoServerAddress}
	// Value: discover.ServiceRegistryEntry
	tsoServiceKey string
	// legacySvcRootPath defines the legacy root path for all etcd paths which derives from
	// the PD/PD service. It's in the format of "/pd/{cluster_id}".
	// The main paths for different usages include:
	// 1. The path, used by the default keyspace group, for LoadTimestamp/SaveTimestamp in the
	//    storage endpoint.
	//    Key: /pd/{cluster_id}/timestamp
	//    Value: ts(time.Time)
	//    Key: /pd/{cluster_id}/lta/{dc-location}/timestamp
	//    Value: ts(time.Time)
	// 2. The path for storing keyspace group membership/distribution metadata.
	//    Key: /pd/{cluster_id}/tso/keyspace_groups/membership/{group}
	//    Value: endpoint.KeyspaceGroup
	// Note: The {group} is 5 digits integer with leading zeros.
	legacySvcRootPath string
	// tsoSvcRootPath defines the root path for all etcd paths used in the tso microservices.
	// It is in the format of "/ms/<cluster-id>/tso".
	// The main paths for different usages include:
	// 1. The path for keyspace group primary election.
	//    default keyspace group: "/ms/{cluster_id}/tso/00000/primary".
	//    non-default keyspace group: "/ms/{cluster_id}/tso/keyspace_groups/election/{group}/primary".
	// 2. The path for LoadTimestamp/SaveTimestamp in the storage endpoint for all the non-default
	//    keyspace groups.
	//    Key: /ms/{cluster_id}/tso/{group}/gta/timestamp
	//    Value: ts(time.Time)
	//    Key: /ms/{cluster_id}/tso/{group}/lta/{dc-location}/timestamp
	//    Value: ts(time.Time)
	// Note: The {group} is 5 digits integer with leading zeros.
	tsoSvcRootPath string
	// legacySvcStorage is storage with legacySvcRootPath.
	legacySvcStorage *endpoint.StorageEndpoint
	// tsoSvcStorage is storage with tsoSvcRootPath.
	tsoSvcStorage *endpoint.StorageEndpoint
	// cfg is the TSO config
	cfg ServiceConfig

	loadKeyspaceGroupsBatchSize int64
	loadFromEtcdMaxRetryTimes   int

	// compiledKGMembershipIDRegexp is the compiled regular expression for matching keyspace group id
	// in the keyspace group membership path.
	compiledKGMembershipIDRegexp *regexp.Regexp
	// groupUpdateRetryList is the list of keyspace groups which failed to update and need to retry.
	groupUpdateRetryList map[uint32]*endpoint.KeyspaceGroup
	groupWatcher         *etcdutil.LoopWatcher

	// mergeCheckerCancelMap is the cancel function map for the merge checker of each keyspace group.
	mergeCheckerCancelMap sync.Map // GroupID -> context.CancelFunc

	primaryPriorityCheckInterval time.Duration

	// tsoNodes is the registered tso servers.
	tsoNodes sync.Map // store as map[string]struct{}
	// serviceRegistryMap stores the mapping from the service registry key to the service address.
	// Note: it is only used in tsoNodesWatcher.
	serviceRegistryMap map[string]string
	// tsoNodesWatcher is the watcher for the registered tso servers.
	tsoNodesWatcher *etcdutil.LoopWatcher

	// pre-initialized metrics
	metrics *keyspaceGroupMetrics
}

// NewKeyspaceGroupManager creates a new Keyspace Group Manager.
func NewKeyspaceGroupManager(
	ctx context.Context,
	tsoServiceID *discovery.ServiceRegistryEntry,
	etcdClient *clientv3.Client,
	httpClient *http.Client,
	electionNamePrefix string,
	legacySvcRootPath string,
	tsoSvcRootPath string,
	cfg ServiceConfig,
) *KeyspaceGroupManager {
	if constant.MaxKeyspaceGroupCountInUse > constant.MaxKeyspaceGroupCount {
		log.Fatal("MaxKeyspaceGroupCountInUse is larger than MaxKeyspaceGroupCount",
			zap.Uint32("max-keyspace-group-count-in-use", constant.MaxKeyspaceGroupCountInUse),
			zap.Uint32("max-keyspace-group-count", constant.MaxKeyspaceGroupCount))
	}

	ctx, cancel := context.WithCancel(ctx)
	kgm := &KeyspaceGroupManager{
		ctx:                          ctx,
		cancel:                       cancel,
		tsoServiceID:                 tsoServiceID,
		etcdClient:                   etcdClient,
		httpClient:                   httpClient,
		electionNamePrefix:           electionNamePrefix,
		tsoServiceKey:                keypath.TSOPath(),
		legacySvcRootPath:            legacySvcRootPath,
		tsoSvcRootPath:               tsoSvcRootPath,
		primaryPriorityCheckInterval: defaultPrimaryPriorityCheckInterval,
		cfg:                          cfg,
		groupUpdateRetryList:         make(map[uint32]*endpoint.KeyspaceGroup),
		serviceRegistryMap:           make(map[string]string),
		metrics:                      newKeyspaceGroupMetrics(),
	}
	kgm.legacySvcStorage = endpoint.NewStorageEndpoint(
		kv.NewEtcdKVBase(kgm.etcdClient, kgm.legacySvcRootPath), nil)
	kgm.tsoSvcStorage = endpoint.NewStorageEndpoint(
		kv.NewEtcdKVBase(kgm.etcdClient, kgm.tsoSvcRootPath), nil)
	kgm.compiledKGMembershipIDRegexp = keypath.GetCompiledKeyspaceGroupIDRegexp()
	kgm.state.initialize()
	return kgm
}

// Initialize this KeyspaceGroupManager
func (kgm *KeyspaceGroupManager) Initialize() error {
	if err := kgm.InitializeTSOServerWatchLoop(); err != nil {
		log.Error("failed to initialize tso server watch loop", zap.Error(err))
		kgm.Close() // Close the manager to clean up the allocated resources.
		return errs.ErrLoadKeyspaceGroupsTerminated.Wrap(err)
	}
	if err := kgm.InitializeGroupWatchLoop(); err != nil {
		log.Error("failed to initialize group watch loop", zap.Error(err))
		kgm.Close() // Close the manager to clean up the loaded keyspace groups.
		return errs.ErrLoadKeyspaceGroupsTerminated.Wrap(err)
	}

	kgm.wg.Add(3)
	go kgm.primaryPriorityCheckLoop()
	go kgm.groupSplitPatroller()
	go kgm.deletedGroupCleaner()

	return nil
}

// Close this KeyspaceGroupManager
func (kgm *KeyspaceGroupManager) Close() {
	log.Info("closing keyspace group manager")

	// Note: don't change the order. We need to cancel all service loops in the keyspace group manager
	// before closing all keyspace groups. It's to prevent concurrent addition/removal of keyspace groups
	// during critical periods such as service shutdown and online keyspace group, while the former requires
	// snapshot isolation to ensure all keyspace groups are properly closed and no new keyspace group is
	// added/initialized after that.
	kgm.cancel()
	kgm.wg.Wait()
	kgm.state.deInitialize()

	log.Info("keyspace group manager closed")
}

// GetServiceConfig returns the service config.
func (kgm *KeyspaceGroupManager) GetServiceConfig() ServiceConfig {
	return kgm.cfg
}

// InitializeTSOServerWatchLoop initializes the watch loop monitoring the path for storing the
// registered tso servers.
// Key: /ms/{cluster_id}/tso/registry/{tsoServerAddress}
// Value: discover.ServiceRegistryEntry
func (kgm *KeyspaceGroupManager) InitializeTSOServerWatchLoop() error {
	putFn := func(kv *mvccpb.KeyValue) error {
		s := &discovery.ServiceRegistryEntry{}
		if err := json.Unmarshal(kv.Value, s); err != nil {
			log.Warn("failed to unmarshal service registry entry",
				zap.String("event-kv-key", string(kv.Key)), zap.Error(err))
			return err
		}
		kgm.tsoNodes.Store(s.ServiceAddr, struct{}{})
		kgm.serviceRegistryMap[string(kv.Key)] = s.ServiceAddr
		return nil
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		if serviceAddr, ok := kgm.serviceRegistryMap[key]; ok {
			delete(kgm.serviceRegistryMap, key)
			kgm.tsoNodes.Delete(serviceAddr)
			return nil
		}
		return perrors.Errorf("failed to find the service address for key %s", key)
	}

	kgm.tsoNodesWatcher = etcdutil.NewLoopWatcher(
		kgm.ctx,
		&kgm.wg,
		kgm.etcdClient,
		"tso-nodes-watcher",
		kgm.tsoServiceKey,
		func([]*clientv3.Event) error { return nil },
		putFn,
		deleteFn,
		func([]*clientv3.Event) error { return nil },
		true, /* withPrefix */
	)
	kgm.tsoNodesWatcher.StartWatchLoop()
	if err := kgm.tsoNodesWatcher.WaitLoad(); err != nil {
		log.Error("failed to load the registered tso servers", errs.ZapError(err))
		return err
	}

	return nil
}

// InitializeGroupWatchLoop initializes the watch loop monitoring the path for storing keyspace group
// membership/distribution metadata.
// Key: /pd/{cluster_id}/tso/keyspace_groups/membership/{group}
// Value: endpoint.KeyspaceGroup
func (kgm *KeyspaceGroupManager) InitializeGroupWatchLoop() error {
	rootPath := kgm.legacySvcRootPath
	startKey := rootPath + "/" + keypath.KeyspaceGroupIDPrefix()

	defaultKGConfigured := false
	putFn := func(kv *mvccpb.KeyValue) error {
		group := &endpoint.KeyspaceGroup{}
		if err := json.Unmarshal(kv.Value, group); err != nil {
			return errs.ErrJSONUnmarshal.Wrap(err)
		}
		kgm.updateKeyspaceGroup(group)
		if group.ID == constant.DefaultKeyspaceGroupID {
			defaultKGConfigured = true
		}
		return nil
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		groupID, err := ExtractKeyspaceGroupIDFromPath(kgm.compiledKGMembershipIDRegexp, string(kv.Key))
		if err != nil {
			return err
		}
		kgm.deleteKeyspaceGroup(groupID)
		return nil
	}
	postEventsFn := func([]*clientv3.Event) error {
		// Retry the groups that are not initialized successfully before.
		for id, group := range kgm.groupUpdateRetryList {
			delete(kgm.groupUpdateRetryList, id)
			kgm.updateKeyspaceGroup(group)
		}
		return nil
	}
	kgm.groupWatcher = etcdutil.NewLoopWatcher(
		kgm.ctx,
		&kgm.wg,
		kgm.etcdClient,
		"keyspace-watcher",
		startKey,
		func([]*clientv3.Event) error { return nil },
		putFn,
		deleteFn,
		postEventsFn,
		true, /* withPrefix */
	)
	if kgm.loadFromEtcdMaxRetryTimes > 0 {
		kgm.groupWatcher.SetLoadRetryTimes(kgm.loadFromEtcdMaxRetryTimes)
	}
	if kgm.loadKeyspaceGroupsBatchSize > 0 {
		kgm.groupWatcher.SetLoadBatchSize(kgm.loadKeyspaceGroupsBatchSize)
	}
	kgm.groupWatcher.StartWatchLoop()
	if err := kgm.groupWatcher.WaitLoad(); err != nil {
		log.Error("failed to initialize keyspace group manager", errs.ZapError(err))
		// We might have partially loaded/initialized the keyspace groups. Close the manager to clean up.
		kgm.Close()
		return errs.ErrLoadKeyspaceGroupsTerminated.Wrap(err)
	}

	if !defaultKGConfigured {
		log.Info("initializing default keyspace group")
		group := &endpoint.KeyspaceGroup{
			ID: constant.DefaultKeyspaceGroupID,
			Members: []endpoint.KeyspaceGroupMember{{
				Address:  kgm.tsoServiceID.ServiceAddr,
				Priority: constant.DefaultKeyspaceGroupReplicaPriority,
			}},
			Keyspaces: []uint32{constant.DefaultKeyspaceID},
		}
		kgm.updateKeyspaceGroup(group)
	}
	return nil
}

func (kgm *KeyspaceGroupManager) primaryPriorityCheckLoop() {
	defer logutil.LogPanic()
	defer kgm.wg.Done()

	failpoint.Inject("fastPrimaryPriorityCheck", func() {
		kgm.primaryPriorityCheckInterval = 200 * time.Millisecond
	})

	ticker := time.NewTicker(kgm.primaryPriorityCheckInterval)
	defer ticker.Stop()
	ctx, cancel := context.WithCancel(kgm.ctx)
	defer cancel()
	groupID := 0
	for {
		select {
		case <-ctx.Done():
			log.Info("exit primary priority check loop")
			return
		case <-ticker.C:
			// Every primaryPriorityCheckInterval, we only reset the primary of one keyspace group
			member, kg, localPriority, nextGroupID := kgm.getNextPrimaryToReset(groupID, kgm.tsoServiceID.ServiceAddr)
			if member != nil {
				aliveTSONodes := make(map[string]struct{})
				kgm.tsoNodes.Range(func(key, _ any) bool {
					aliveTSONodes[typeutil.TrimScheme(key.(string))] = struct{}{}
					return true
				})
				if len(aliveTSONodes) == 0 {
					log.Warn("no alive tso node", zap.String("local-address", kgm.tsoServiceID.ServiceAddr))
					continue
				}
				// If there is a alive member with higher priority, reset the leader.
				resetLeader := false
				for _, m := range kg.Members {
					if m.Priority <= localPriority {
						continue
					}
					if _, ok := aliveTSONodes[typeutil.TrimScheme(m.Address)]; ok {
						resetLeader = true
						break
					}
				}
				if resetLeader {
					select {
					case <-ctx.Done():
					default:
						allocator, err := kgm.GetAllocatorManager(kg.ID)
						if err != nil {
							log.Error("failed to get allocator manager", zap.Error(err))
							continue
						}
						// only members of specific group are valid primary candidates.
						group := kgm.GetKeyspaceGroups()[kg.ID]
						memberMap := make(map[string]bool, len(group.Members))
						for _, m := range group.Members {
							memberMap[m.Address] = true
						}
						log.Info("tso priority checker moves primary",
							zap.String("local-address", kgm.tsoServiceID.ServiceAddr),
							zap.Uint32("keyspace-group-id", kg.ID),
							zap.Int("local-priority", localPriority))
						if err := utils.TransferPrimary(kgm.etcdClient, allocator.GetAllocator().(*GlobalTSOAllocator).GetExpectedPrimaryLease(),
							constant.TSOServiceName, kgm.GetServiceConfig().GetName(), "", kg.ID, memberMap); err != nil {
							log.Error("failed to transfer primary", zap.Error(err))
							continue
						}
					}
				} else {
					log.Warn("no need to reset primary as the replicas with higher priority are offline",
						zap.String("local-address", kgm.tsoServiceID.ServiceAddr),
						zap.Uint32("keyspace-group-id", kg.ID),
						zap.Int("local-priority", localPriority))
				}
			}
			groupID = nextGroupID
		}
	}
}

func (kgm *KeyspaceGroupManager) isAssignedToMe(group *endpoint.KeyspaceGroup) bool {
	return slice.AnyOf(group.Members, func(i int) bool {
		return group.Members[i].IsAddressEquivalent(kgm.tsoServiceID.ServiceAddr)
	})
}

// updateKeyspaceGroup applies the given keyspace group. If the keyspace group is just assigned to
// this host/pod, it will join the primary election.
func (kgm *KeyspaceGroupManager) updateKeyspaceGroup(group *endpoint.KeyspaceGroup) {
	if err := checkKeySpaceGroupID(group.ID); err != nil {
		log.Warn("keyspace group ID is invalid, ignore it", zap.Error(err))
		return
	}

	// If the default keyspace group isn't assigned to any tso node/pod, assign it to everyone.
	if group.ID == constant.DefaultKeyspaceGroupID && len(group.Members) == 0 {
		// TODO: fill members with all tso nodes/pods.
		group.Members = []endpoint.KeyspaceGroupMember{{
			Address:  kgm.tsoServiceID.ServiceAddr,
			Priority: constant.DefaultKeyspaceGroupReplicaPriority,
		}}
	}

	if !kgm.isAssignedToMe(group) {
		// Not assigned to me. If this host/pod owns a replica of this keyspace group,
		// it should resign the election membership now.
		kgm.exitElectionMembership(group)
		return
	}

	oldAM, oldGroup := kgm.getKeyspaceGroupMeta(group.ID)
	// If this host owns a replica of the keyspace group which is the merge target,
	// it should run the merging checker when the merge state first time changes.
	if !oldGroup.IsMergeTarget() && group.IsMergeTarget() {
		ctx, cancel := context.WithCancel(kgm.ctx)
		kgm.mergeCheckerCancelMap.Store(group.ID, cancel)
		kgm.wg.Add(1)
		go kgm.mergingChecker(ctx, group.ID, group.MergeState.MergeList)
		kgm.metrics.mergeTargetGauge.Inc()
		kgm.metrics.mergeSourceGauge.Add(float64(len(group.MergeState.MergeList)))
	}
	// If the merge state has been finished, cancel its merging checker.
	if oldGroup.IsMergeTarget() && !group.IsMergeTarget() {
		if cancel, loaded := kgm.mergeCheckerCancelMap.LoadAndDelete(group.ID); loaded && cancel != nil {
			cancel.(context.CancelFunc)()
		}
		kgm.metrics.mergeTargetGauge.Dec()
	}

	// If this host is already assigned a replica of this keyspace group, i.e., the election member
	// is already initialized, just update the meta.
	if oldAM != nil {
		kgm.updateKeyspaceGroupMembership(oldGroup, group, true)
		return
	}

	// If the keyspace group is not initialized, initialize it.
	// The format of leader name is address-groupID.
	uniqueName := fmt.Sprintf("%s-%05d", kgm.electionNamePrefix, group.ID)
	uniqueID := memberutil.GenerateUniqueID(uniqueName)
	log.Info("joining primary election",
		zap.Uint32("keyspace-group-id", group.ID),
		zap.String("participant-name", uniqueName),
		zap.Uint64("participant-id", uniqueID))
	// Initialize the participant info to join the primary election.
	participant := member.NewParticipant(kgm.etcdClient, keypath.MsParam{
		ServiceName: constant.TSOServiceName,
		GroupID:     group.ID,
	})
	p := &tsopb.Participant{
		Name:       uniqueName,
		Id:         uniqueID, // id is unique among all participants
		ListenUrls: []string{kgm.cfg.GetAdvertiseListenAddr()},
	}
	participant.InitInfo(p, "keyspace group primary election")
	// If the keyspace group is in split, we should ensure that the primary elected by the new keyspace group
	// is always on the same TSO Server node as the primary of the old keyspace group, and this constraint cannot
	// be broken until the entire split process is completed.
	if group.IsSplitTarget() {
		splitSource := group.SplitSource()
		log.Info("keyspace group is in split",
			zap.Uint32("target", group.ID),
			zap.Uint32("source", splitSource))
		splitSourceAM, splitSourceGroup := kgm.getKeyspaceGroupMeta(splitSource)
		if !validateSplit(splitSourceAM, group, splitSourceGroup) {
			// Put the group into the retry list to retry later.
			kgm.groupUpdateRetryList[group.ID] = group
			return
		}
		participant.SetCampaignChecker(func(*election.Leadership) bool {
			return splitSourceAM.GetMember().IsLeader()
		})
	}
	// Only the default keyspace group uses the legacy service root path for LoadTimestamp/SyncTimestamp.
	var (
		tsRootPath string
		storage    *endpoint.StorageEndpoint
	)
	if group.ID == constant.DefaultKeyspaceGroupID {
		tsRootPath = kgm.legacySvcRootPath
		storage = kgm.legacySvcStorage
	} else {
		tsRootPath = kgm.tsoSvcRootPath
		storage = kgm.tsoSvcStorage
	}
	// Initialize all kinds of maps.
	am := NewAllocatorManager(kgm.ctx, group.ID, participant, tsRootPath, storage, kgm.cfg)
	am.startGlobalAllocatorLoop()
	log.Info("created allocator manager",
		zap.Uint32("keyspace-group-id", group.ID))
	kgm.Lock()
	group.KeyspaceLookupTable = make(map[uint32]struct{})
	for _, kid := range group.Keyspaces {
		group.KeyspaceLookupTable[kid] = struct{}{}
		kgm.keyspaceLookupTable[kid] = group.ID
	}
	kgm.kgs[group.ID] = group
	kgm.ams[group.ID] = am
	// If the group is the split target, add it to the splitting group map.
	if group.IsSplitTarget() {
		kgm.splittingGroups[group.ID] = time.Now()
		kgm.metrics.splitTargetGauge.Inc()
	}
	kgm.Unlock()
}

// validateSplit checks whether the meta info of split keyspace group
// to ensure that the split process could be continued.
func validateSplit(
	sourceAM *AllocatorManager,
	targetGroup, sourceGroup *endpoint.KeyspaceGroup,
) bool {
	splitSourceID := targetGroup.SplitSource()
	// Make sure that the split source keyspace group has been initialized.
	if sourceAM == nil || sourceGroup == nil {
		log.Error("the split source keyspace group is not initialized",
			zap.Uint32("target", targetGroup.ID),
			zap.Uint32("source", splitSourceID))
		return false
	}
	// Since the target group is derived from the source group and both of them
	// could not be modified during the split process, so we can only check the
	// member count of the source group here.
	memberCount := len(sourceGroup.Members)
	if memberCount < constant.DefaultKeyspaceGroupReplicaCount {
		log.Error("the split source keyspace group does not have enough members",
			zap.Uint32("target", targetGroup.ID),
			zap.Uint32("source", splitSourceID),
			zap.Int("member-count", memberCount),
			zap.Int("replica-count", constant.DefaultKeyspaceGroupReplicaCount))
		return false
	}
	return true
}

// updateKeyspaceGroupMembership updates the keyspace lookup table for the given keyspace group.
func (kgm *KeyspaceGroupManager) updateKeyspaceGroupMembership(
	oldGroup, newGroup *endpoint.KeyspaceGroup, updateWithLock bool,
) {
	var (
		oldKeyspaces           []uint32
		oldKeyspaceLookupTable map[uint32]struct{}
	)

	if oldGroup != nil {
		oldKeyspaces = oldGroup.Keyspaces
		oldKeyspaceLookupTable = oldGroup.KeyspaceLookupTable
	}

	groupID := newGroup.ID
	newKeyspaces := newGroup.Keyspaces
	oldLen, newLen := len(oldKeyspaces), len(newKeyspaces)

	// Sort the keyspaces in ascending order
	sort.Slice(newKeyspaces, func(i, j int) bool {
		return newKeyspaces[i] < newKeyspaces[j]
	})

	// Mostly, the membership has no change, so optimize for this case.
	sameMembership := true
	if oldLen != newLen {
		sameMembership = false
	} else {
		for i := range oldLen {
			if oldKeyspaces[i] != newKeyspaces[i] {
				sameMembership = false
				break
			}
		}
	}

	if updateWithLock {
		kgm.Lock()
		defer kgm.Unlock()
	}

	if sameMembership {
		// The keyspace group membership is not changed. Reuse the old one.
		newGroup.KeyspaceLookupTable = oldKeyspaceLookupTable
	} else {
		// The keyspace list might be too long, so we only log the length, though there is a rare case that
		// the old length and the new length are the same but the keyspace list is changed.
		log.Info("the keyspace group's keyspace list is changed",
			zap.Uint32("keyspace-group-id", groupID),
			zap.Int("old-keyspaces-count", oldLen),
			zap.Int("new-keyspaces-count", newLen))
		// The keyspace group membership is changed. Update the keyspace lookup table.
		newGroup.KeyspaceLookupTable = make(map[uint32]struct{})
		for i, j := 0, 0; i < oldLen || j < newLen; {
			if i < oldLen && j < newLen && oldKeyspaces[i] == newKeyspaces[j] {
				newGroup.KeyspaceLookupTable[newKeyspaces[j]] = struct{}{}
				i++
				j++
			} else if i < oldLen && j < newLen && oldKeyspaces[i] < newKeyspaces[j] || j == newLen {
				// kgm.keyspaceLookupTable is a global lookup table for all keyspace groups, storing the
				// keyspace group ID for each keyspace. If the keyspace group of this keyspace in this
				// lookup table isn't the current keyspace group, it means the keyspace has been moved
				// to another keyspace group which has already declared the ownership of the keyspace,
				// and we shouldn't delete and overwrite the ownership.
				if curGroupID, ok := kgm.keyspaceLookupTable[oldKeyspaces[i]]; ok && curGroupID == groupID {
					delete(kgm.keyspaceLookupTable, oldKeyspaces[i])
				}
				i++
			} else {
				newGroup.KeyspaceLookupTable[newKeyspaces[j]] = struct{}{}
				kgm.keyspaceLookupTable[newKeyspaces[j]] = groupID
				j++
			}
		}
		if groupID == constant.DefaultKeyspaceGroupID {
			if _, ok := newGroup.KeyspaceLookupTable[constant.DefaultKeyspaceID]; !ok {
				log.Warn("default keyspace is not in default keyspace group. add it back")
				kgm.keyspaceLookupTable[constant.DefaultKeyspaceID] = groupID
				newGroup.KeyspaceLookupTable[constant.DefaultKeyspaceID] = struct{}{}
				newGroup.Keyspaces = make([]uint32, 1+len(newKeyspaces))
				newGroup.Keyspaces[0] = constant.DefaultKeyspaceID
				copy(newGroup.Keyspaces[1:], newKeyspaces)
			}
		} else {
			if _, ok := newGroup.KeyspaceLookupTable[constant.DefaultKeyspaceID]; ok {
				log.Warn("default keyspace is in non-default keyspace group. remove it")
				kgm.keyspaceLookupTable[constant.DefaultKeyspaceID] = constant.DefaultKeyspaceGroupID
				delete(newGroup.KeyspaceLookupTable, constant.DefaultKeyspaceID)
				newGroup.Keyspaces = newKeyspaces[1:]
			}
		}
	}
	// Check the split state.
	if oldGroup != nil {
		// SplitTarget -> !Splitting
		if oldGroup.IsSplitTarget() && !newGroup.IsSplitting() {
			kgm.ams[groupID].GetMember().(*member.Participant).SetCampaignChecker(nil)
			splitTime := kgm.splittingGroups[groupID]
			delete(kgm.splittingGroups, groupID)
			kgm.metrics.splitTargetGauge.Dec()
			kgm.metrics.splitDuration.Observe(time.Since(splitTime).Seconds())
		}
		// SplitSource -> !SplitSource
		if oldGroup.IsSplitSource() && !newGroup.IsSplitting() {
			kgm.metrics.splitSourceGauge.Dec()
		}
		// !Splitting -> SplitSource
		if !oldGroup.IsSplitting() && newGroup.IsSplitSource() {
			kgm.metrics.splitSourceGauge.Inc()
		}
	}
	kgm.kgs[groupID] = newGroup
}

// deleteKeyspaceGroup deletes the given keyspace group.
func (kgm *KeyspaceGroupManager) deleteKeyspaceGroup(groupID uint32) {
	log.Info("delete keyspace group", zap.Uint32("keyspace-group-id", groupID))

	if groupID == constant.DefaultKeyspaceGroupID {
		log.Info("removed default keyspace group meta config from the storage. " +
			"now every tso node/pod will initialize it")
		group := &endpoint.KeyspaceGroup{
			ID: constant.DefaultKeyspaceGroupID,
			Members: []endpoint.KeyspaceGroupMember{{
				Address:  kgm.tsoServiceID.ServiceAddr,
				Priority: constant.DefaultKeyspaceGroupReplicaPriority,
			}},
			Keyspaces: []uint32{constant.DefaultKeyspaceID},
		}
		kgm.updateKeyspaceGroup(group)
		return
	}

	kgm.Lock()
	defer kgm.Unlock()

	kg := kgm.kgs[groupID]
	if kg != nil {
		for _, kid := range kg.Keyspaces {
			// if kid == kg.ID, it means the keyspace still belongs to this keyspace group,
			//     so we decouple the relationship in the global keyspace lookup table.
			// if kid != kg.ID, it means the keyspace has been moved to another keyspace group
			//     which has already declared the ownership of the keyspace, so we don't need
			//     delete it from the global keyspace lookup table and overwrite the ownership.
			if kid == kg.ID {
				delete(kgm.keyspaceLookupTable, kid)
			}
		}
		kgm.kgs[groupID] = nil
	}

	am := kgm.ams[groupID]
	if am != nil {
		am.close()
		kgm.ams[groupID] = nil
	}

	kgm.deletedGroups[groupID] = struct{}{}
}

// exitElectionMembership exits the election membership of the given keyspace group by
// de-initializing the allocator manager, but still keeps the keyspace group info.
func (kgm *KeyspaceGroupManager) exitElectionMembership(group *endpoint.KeyspaceGroup) {
	log.Info("resign election membership", zap.Uint32("keyspace-group-id", group.ID))

	kgm.Lock()
	defer kgm.Unlock()

	am := kgm.ams[group.ID]
	if am != nil {
		am.close()
		kgm.ams[group.ID] = nil
	}

	oldGroup := kgm.kgs[group.ID]
	kgm.updateKeyspaceGroupMembership(oldGroup, group, false)
}

// GetAllocatorManager returns the AllocatorManager of the given keyspace group
func (kgm *KeyspaceGroupManager) GetAllocatorManager(keyspaceGroupID uint32) (*AllocatorManager, error) {
	if err := checkKeySpaceGroupID(keyspaceGroupID); err != nil {
		return nil, err
	}
	if am, _ := kgm.getKeyspaceGroupMeta(keyspaceGroupID); am != nil {
		return am, nil
	}
	return nil, genNotServedErr(errs.ErrGetAllocatorManager, keyspaceGroupID)
}

// FindGroupByKeyspaceID returns the keyspace group that contains the keyspace with the given ID.
func (kgm *KeyspaceGroupManager) FindGroupByKeyspaceID(
	keyspaceID uint32,
) (*AllocatorManager, *endpoint.KeyspaceGroup, uint32, error) {
	curAM, curKeyspaceGroup, curKeyspaceGroupID, err :=
		kgm.getKeyspaceGroupMetaWithCheck(keyspaceID, constant.DefaultKeyspaceGroupID)
	if err != nil {
		return nil, nil, curKeyspaceGroupID, err
	}
	return curAM, curKeyspaceGroup, curKeyspaceGroupID, nil
}

// GetElectionMember returns the election member of the keyspace group serving the given keyspace.
func (kgm *KeyspaceGroupManager) GetElectionMember(
	keyspaceID, keyspaceGroupID uint32,
) (ElectionMember, error) {
	if err := checkKeySpaceGroupID(keyspaceGroupID); err != nil {
		return nil, err
	}
	am, _, _, err := kgm.getKeyspaceGroupMetaWithCheck(keyspaceID, keyspaceGroupID)
	if err != nil {
		return nil, err
	}
	return am.GetMember(), nil
}

// GetKeyspaceGroups returns all keyspace groups managed by the current keyspace group manager.
func (kgm *KeyspaceGroupManager) GetKeyspaceGroups() map[uint32]*endpoint.KeyspaceGroup {
	kgm.RLock()
	defer kgm.RUnlock()
	keyspaceGroups := make(map[uint32]*endpoint.KeyspaceGroup)
	for _, keyspaceGroupID := range kgm.keyspaceLookupTable {
		if _, ok := keyspaceGroups[keyspaceGroupID]; ok {
			continue
		}
		keyspaceGroups[keyspaceGroupID] = kgm.kgs[keyspaceGroupID]
	}
	return keyspaceGroups
}

// HandleTSORequest forwards TSO allocation requests to correct TSO Allocators of the given keyspace group.
func (kgm *KeyspaceGroupManager) HandleTSORequest(
	ctx context.Context,
	keyspaceID, keyspaceGroupID uint32,
	count uint32,
) (ts pdpb.Timestamp, curKeyspaceGroupID uint32, err error) {
	if err := checkKeySpaceGroupID(keyspaceGroupID); err != nil {
		return pdpb.Timestamp{}, keyspaceGroupID, err
	}
	am, _, curKeyspaceGroupID, err := kgm.getKeyspaceGroupMetaWithCheck(keyspaceID, keyspaceGroupID)
	if err != nil {
		return pdpb.Timestamp{}, curKeyspaceGroupID, err
	}
	err = kgm.checkTSOSplit(curKeyspaceGroupID)
	if err != nil {
		return pdpb.Timestamp{}, curKeyspaceGroupID, err
	}
	err = kgm.state.checkGroupMerge(curKeyspaceGroupID)
	if err != nil {
		return pdpb.Timestamp{}, curKeyspaceGroupID, err
	}
	// If this is the first time to request the keyspace group, we need to sync the
	// timestamp one more time before serving the TSO request to make sure that the
	// TSO is the latest one from the storage, which could prevent the potential
	// fallback caused by the rolling update of the mixed old PD and TSO service deployment.
	err = kgm.markGroupRequested(curKeyspaceGroupID, func() error {
		return am.GetAllocator().Initialize(0)
	})
	if err != nil {
		return pdpb.Timestamp{}, curKeyspaceGroupID, err
	}
	ts, err = am.HandleRequest(ctx, count)
	return ts, curKeyspaceGroupID, err
}

func checkKeySpaceGroupID(id uint32) error {
	if id < constant.MaxKeyspaceGroupCountInUse {
		return nil
	}
	return errs.ErrKeyspaceGroupIDInvalid.FastGenByArgs(
		fmt.Sprintf("%d shouldn't >= %d", id, constant.MaxKeyspaceGroupCountInUse))
}

// GetMinTS returns the minimum timestamp across all keyspace groups served by this TSO server/pod.
func (kgm *KeyspaceGroupManager) GetMinTS() (_ pdpb.Timestamp, kgAskedCount, kgTotalCount uint32, err error) {
	kgm.RLock()
	defer kgm.RUnlock()

	var minTS *pdpb.Timestamp
	for i, am := range kgm.ams {
		if kgm.kgs[i] != nil {
			kgTotalCount++
		}
		// If any keyspace group hasn't elected primary, we can't know its current timestamp of
		// the group, so as to the min ts across all keyspace groups. Return error in this case.
		if am != nil && !am.member.IsLeaderElected() {
			return pdpb.Timestamp{}, kgAskedCount, kgTotalCount, errs.ErrGetMinTS.FastGenByArgs("leader is not elected")
		}
		// Skip the keyspace groups that are not served by this TSO Server/Pod.
		if am == nil || !am.IsLeader() {
			continue
		}
		kgAskedCount++
		// Skip the keyspace groups that are split targets, because they always have newer
		// time lines than the existing split sources thus won't contribute to the min ts.
		if kgm.kgs[i] != nil && kgm.kgs[i].IsSplitTarget() {
			continue
		}
		ts, err := am.HandleRequest(context.Background(), 1)
		if err != nil {
			return pdpb.Timestamp{}, kgAskedCount, kgTotalCount, err
		}
		if minTS == nil || tsoutil.CompareTimestamp(&ts, minTS) < 0 {
			minTS = &ts
		}
	}

	if minTS == nil {
		// This TSO server/pod is not serving any keyspace group, return an empty timestamp,
		// and the client needs to skip the empty timestamps when collecting the min timestamp
		// from all TSO servers/pods.
		return pdpb.Timestamp{}, kgAskedCount, kgTotalCount, nil
	}

	return *minTS, kgAskedCount, kgTotalCount, nil
}

func genNotServedErr(perr *perrors.Error, keyspaceGroupID uint32) error {
	return perr.FastGenByArgs(
		fmt.Sprintf(
			"requested keyspace group with id %d %s by this host/pod",
			keyspaceGroupID, errs.NotServedErr))
}

// checkTSOSplit checks if the given keyspace group is in split state, and if so, it will make sure the
// newly split TSO keep consistent with the original one.
func (kgm *KeyspaceGroupManager) checkTSOSplit(
	keyspaceGroupID uint32,
) error {
	splitTargetAM, splitSourceAM, err := kgm.state.checkGroupSplit(keyspaceGroupID)
	if err != nil || splitTargetAM == nil {
		return err
	}
	splitTargetAllocator := splitTargetAM.GetAllocator()
	splitSourceAllocator := splitSourceAM.GetAllocator()
	splitTargetTSO, err := splitTargetAllocator.GenerateTSO(context.Background(), 1)
	if err != nil {
		return err
	}
	splitSourceTSO, err := splitSourceAllocator.GenerateTSO(context.Background(), 1)
	if err != nil {
		return err
	}
	// If the split source TSO is not greater than the newly split TSO, we don't need to do anything.
	if tsoutil.CompareTimestamp(&splitSourceTSO, &splitTargetTSO) <= 0 {
		log.Info("the split source tso is less than the newly split tso",
			zap.Int64("split-source-tso-physical", splitSourceTSO.Physical),
			zap.Int64("split-source-tso-logical", splitSourceTSO.Logical),
			zap.Int64("split-tso-physical", splitTargetTSO.Physical),
			zap.Int64("split-tso-logical", splitTargetTSO.Logical))
		// Finish the split state directly.
		return kgm.finishSplitKeyspaceGroup(keyspaceGroupID)
	}
	// If the split source TSO is greater than the newly split TSO, we need to update the split
	// TSO to make sure the following TSO will be greater than the split keyspaces ever had
	// in the past.
	err = splitTargetAllocator.SetTSO(tsoutil.GenerateTS(&pdpb.Timestamp{
		Physical: splitSourceTSO.Physical + 1,
		Logical:  splitSourceTSO.Logical,
	}), true, true)
	if err != nil {
		return err
	}
	log.Info("the split source tso is greater than the newly split tso",
		zap.Int64("split-source-tso-physical", splitSourceTSO.Physical),
		zap.Int64("split-source-tso-logical", splitSourceTSO.Logical),
		zap.Int64("split-tso-physical", splitTargetTSO.Physical),
		zap.Int64("split-tso-logical", splitTargetTSO.Logical))
	// Finish the split state.
	return kgm.finishSplitKeyspaceGroup(keyspaceGroupID)
}

const keyspaceGroupsAPIPrefix = "/pd/api/v2/tso/keyspace-groups"

// Put the code below into the critical section to prevent from sending too many HTTP requests.
func (kgm *KeyspaceGroupManager) finishSplitKeyspaceGroup(id uint32) error {
	start := time.Now()
	kgm.Lock()
	defer kgm.Unlock()
	// Check if the keyspace group is in split state.
	splitGroup := kgm.kgs[id]
	if !splitGroup.IsSplitTarget() {
		return nil
	}
	// Check if the HTTP client is initialized.
	if kgm.httpClient == nil {
		return nil
	}
	startRequest := time.Now()
	resp, err := apiutil.DoDelete(
		kgm.httpClient,
		kgm.cfg.GeBackendEndpoints()+keyspaceGroupsAPIPrefix+fmt.Sprintf("/%d/split", id))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Warn("failed to finish split keyspace group",
			zap.Uint32("keyspace-group-id", id),
			zap.Int("status-code", resp.StatusCode))
		return errs.ErrSendRequest.FastGenByArgs()
	}
	kgm.metrics.finishSplitSendDuration.Observe(time.Since(startRequest).Seconds())
	// Pre-update the split keyspace group's split state in memory.
	// Note: to avoid data race with state read APIs, we always replace the group in memory as a whole.
	// For now, we only have scenarios to update split state/merge state, and the other fields are always
	// loaded from etcd without any modification, so we can simply copy the group and replace the state.
	newSplitGroup := *splitGroup
	newSplitGroup.SplitState = nil
	kgm.kgs[id] = &newSplitGroup
	kgm.metrics.finishSplitDuration.Observe(time.Since(start).Seconds())
	return nil
}

func (kgm *KeyspaceGroupManager) finishMergeKeyspaceGroup(id uint32) error {
	start := time.Now()
	kgm.Lock()
	defer kgm.Unlock()
	// Check if the keyspace group is in the merging state.
	mergeTarget := kgm.kgs[id]
	if !mergeTarget.IsMergeTarget() {
		return nil
	}
	// Check if the HTTP client is initialized.
	if kgm.httpClient == nil {
		return nil
	}
	startRequest := time.Now()
	resp, err := apiutil.DoDelete(
		kgm.httpClient,
		kgm.cfg.GeBackendEndpoints()+keyspaceGroupsAPIPrefix+fmt.Sprintf("/%d/merge", id))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Warn("failed to finish merging keyspace group",
			zap.Uint32("keyspace-group-id", id),
			zap.Int("status-code", resp.StatusCode))
		return errs.ErrSendRequest.FastGenByArgs()
	}
	kgm.metrics.finishMergeSendDuration.Observe(time.Since(startRequest).Seconds())
	// Pre-update the merge target keyspace group's merge state in memory.
	// Note: to avoid data race with state read APIs, we always replace the group in memory as a whole.
	// For now, we only have scenarios to update split state/merge state, and the other fields are always
	// loaded from etcd without any modification, so we can simply copy the group and replace the state.
	newTargetGroup := *mergeTarget
	newTargetGroup.MergeState = nil
	kgm.kgs[id] = &newTargetGroup
	kgm.metrics.finishMergeDuration.Observe(time.Since(start).Seconds())
	return nil
}

// mergingChecker is used to check if the keyspace group is in merge state, and if so, it will
// make sure the newly merged TSO keep consistent with the original ones.
func (kgm *KeyspaceGroupManager) mergingChecker(ctx context.Context, mergeTargetID uint32, mergeList []uint32) {
	startTime := time.Now()
	log.Info("start to merge the keyspace group",
		zap.String("member", kgm.tsoServiceID.ServiceAddr),
		zap.Uint32("merge-target-id", mergeTargetID),
		zap.Uint32s("merge-list", mergeList))
	defer logutil.LogPanic()
	defer kgm.wg.Done()

	checkTicker := time.NewTicker(mergingCheckInterval)
	defer checkTicker.Stop()
	// Prepare the merge map.
	mergeMap := make(map[uint32]struct{}, len(mergeList))
	for _, id := range mergeList {
		mergeMap[id] = struct{}{}
	}

mergeLoop:
	for {
		select {
		case <-ctx.Done():
			log.Info("merging checker is closed",
				zap.String("member", kgm.tsoServiceID.ServiceAddr),
				zap.Uint32("merge-target-id", mergeTargetID),
				zap.Uint32s("merge-list", mergeList))
			return
		case <-checkTicker.C:
		}
		// Check if current TSO node is the merge target TSO primary node.
		am, err := kgm.GetAllocatorManager(mergeTargetID)
		if err != nil {
			log.Warn("unable to get the merge target allocator manager",
				zap.String("member", kgm.tsoServiceID.ServiceAddr),
				zap.Uint32("keyspace-group-id", mergeTargetID),
				zap.Uint32s("merge-list", mergeList),
				zap.Error(err))
			continue
		}
		// If the current TSO node is not the merge target TSO primary node,
		// we still need to keep this loop running to avoid unexpected primary changes.
		if !am.IsLeader() {
			log.Debug("current tso node is not the merge target primary",
				zap.String("member", kgm.tsoServiceID.ServiceAddr),
				zap.Uint32("merge-target-id", mergeTargetID),
				zap.Uint32s("merge-list", mergeList))
			continue
		}
		// Check if the keyspace group primaries in the merge map are all gone.
		if len(mergeMap) != 0 {
			for id := range mergeMap {
				leaderPath := keypath.LeaderPath(&keypath.MsParam{
					ServiceName: constant.TSOServiceName,
					GroupID:     id,
				})
				val, err := kgm.tsoSvcStorage.Load(leaderPath)
				if err != nil {
					log.Error("failed to check if the keyspace group primary in the merge list has gone",
						zap.String("member", kgm.tsoServiceID.ServiceAddr),
						zap.Uint32("merge-target-id", mergeTargetID),
						zap.Uint32s("merge-list", mergeList),
						zap.Uint32("merge-id", id),
						zap.Any("remaining", mergeMap),
						zap.Error(err))
					continue
				}
				if len(val) == 0 {
					delete(mergeMap, id)
				}
			}
		}
		if len(mergeMap) > 0 {
			continue
		}
		kgm.metrics.mergeSourceGauge.Add(-float64(len(mergeList)))
		log.Info("all the keyspace group primaries in the merge list are gone, "+
			"start to calculate the newly merged TSO",
			zap.String("member", kgm.tsoServiceID.ServiceAddr),
			zap.Uint32("merge-target-id", mergeTargetID),
			zap.Uint32s("merge-list", mergeList))
		// All the keyspace group primaries in the merge list are gone,
		// calculate the newly merged TSO to make sure it is greater than the original ones.
		var mergedTS time.Time
		for _, id := range mergeList {
			ts, err := kgm.tsoSvcStorage.LoadTimestamp(keypath.KeyspaceGroupGlobalTSPath(id))
			if err != nil {
				log.Error("failed to load the keyspace group TSO",
					zap.String("member", kgm.tsoServiceID.ServiceAddr),
					zap.Uint32("merge-target-id", mergeTargetID),
					zap.Uint32s("merge-list", mergeList),
					zap.Uint32("merge-id", id),
					zap.Time("ts", ts),
					zap.Error(err))
				// Retry from the beginning of the loop.
				continue mergeLoop
			}
			if ts.After(mergedTS) {
				mergedTS = ts
			}
		}
		// Update the newly merged TSO if the merged TSO is not zero.
		if mergedTS != typeutil.ZeroTime {
			log.Info("start to set the newly merged TSO",
				zap.String("member", kgm.tsoServiceID.ServiceAddr),
				zap.Uint32("merge-target-id", mergeTargetID),
				zap.Uint32s("merge-list", mergeList),
				zap.Time("merged-ts", mergedTS))
			err = am.GetAllocator().SetTSO(
				tsoutil.GenerateTS(tsoutil.GenerateTimestamp(mergedTS, 1)),
				true, true)
			if err != nil {
				log.Error("failed to update the newly merged TSO",
					zap.String("member", kgm.tsoServiceID.ServiceAddr),
					zap.Uint32("merge-target-id", mergeTargetID),
					zap.Uint32s("merge-list", mergeList),
					zap.Time("merged-ts", mergedTS),
					zap.Error(err))
				continue
			}
		}
		// Finish the merge.
		err = kgm.finishMergeKeyspaceGroup(mergeTargetID)
		if err != nil {
			log.Error("failed to finish the merge",
				zap.String("member", kgm.tsoServiceID.ServiceAddr),
				zap.Uint32("merge-target-id", mergeTargetID),
				zap.Uint32s("merge-list", mergeList),
				zap.Error(err))
			continue
		}
		kgm.metrics.mergeDuration.Observe(time.Since(startTime).Seconds())
		log.Info("finished merging keyspace group",
			zap.String("member", kgm.tsoServiceID.ServiceAddr),
			zap.Uint32("merge-target-id", mergeTargetID),
			zap.Uint32s("merge-list", mergeList),
			zap.Time("merged-ts", mergedTS))
		return
	}
}

// groupSplitPatroller is used to patrol the groups that are in the on-going
// split state and to check if we could speed up the split process.
func (kgm *KeyspaceGroupManager) groupSplitPatroller() {
	defer logutil.LogPanic()
	defer kgm.wg.Done()
	patrolInterval := groupPatrolInterval
	failpoint.Inject("fastGroupSplitPatroller", func() {
		patrolInterval = 3 * time.Second
	})
	ticker := time.NewTicker(patrolInterval)
	defer ticker.Stop()
	log.Info("group split patroller is started",
		zap.Duration("patrol-interval", patrolInterval))
	for {
		select {
		case <-kgm.ctx.Done():
			log.Info("group split patroller exited")
			return
		case <-ticker.C:
		}
		for _, groupID := range kgm.getSplittingGroups() {
			am, group := kgm.getKeyspaceGroupMeta(groupID)
			if !am.IsLeader() {
				continue
			}
			if len(group.Keyspaces) == 0 {
				log.Warn("abnormal keyspace group with empty keyspace list",
					zap.Uint32("keyspace-group-id", groupID))
				continue
			}
			log.Info("request tso for the splitting keyspace group",
				zap.Uint32("keyspace-group-id", groupID),
				zap.Uint32("keyspace-id", group.Keyspaces[0]))
			// Request the TSO manually to speed up the split process.
			_, _, err := kgm.HandleTSORequest(kgm.ctx, group.Keyspaces[0], groupID, 1)
			if err != nil {
				log.Warn("failed to request tso for the splitting keyspace group",
					zap.Uint32("keyspace-group-id", groupID),
					zap.Uint32("keyspace-id", group.Keyspaces[0]),
					zap.Error(err))
				continue
			}
		}
	}
}

// deletedGroupCleaner is used to clean the deleted keyspace groups related data.
// For example, the TSO keys of the merged keyspace groups remain in the storage.
func (kgm *KeyspaceGroupManager) deletedGroupCleaner() {
	defer logutil.LogPanic()
	defer kgm.wg.Done()
	patrolInterval := groupPatrolInterval
	failpoint.Inject("fastDeletedGroupCleaner", func() {
		patrolInterval = 200 * time.Millisecond
	})
	ticker := time.NewTicker(patrolInterval)
	defer ticker.Stop()
	log.Info("deleted group cleaner is started",
		zap.Duration("patrol-interval", patrolInterval))
	var (
		empty               = true
		lastDeletedGroupID  uint32
		lastDeletedGroupNum int
	)
	for {
		select {
		case <-kgm.ctx.Done():
			log.Info("deleted group cleaner exited")
			return
		case <-ticker.C:
		}
		for _, groupID := range kgm.getDeletedGroups() {
			// Do not clean the default keyspace group data.
			if groupID == constant.DefaultKeyspaceGroupID {
				continue
			}
			empty = false
			// Make sure the allocator and group meta are not in use anymore.
			am, _ := kgm.getKeyspaceGroupMeta(groupID)
			if am != nil {
				log.Info("the keyspace group tso allocator has not been closed yet",
					zap.Uint32("keyspace-group-id", groupID))
				continue
			}
			log.Info("delete the keyspace group tso key",
				zap.Uint32("keyspace-group-id", groupID))
			// Clean up the remaining TSO keys.
			err := kgm.tsoSvcStorage.DeleteTimestamp(
				keypath.TimestampPath(
					keypath.KeyspaceGroupGlobalTSPath(groupID),
				),
			)
			if err != nil {
				log.Warn("failed to delete the keyspace group tso key",
					zap.Uint32("keyspace-group-id", groupID),
					zap.Error(err))
				continue
			}
			kgm.cleanKeyspaceGroup(groupID)
			lastDeletedGroupID = groupID
			lastDeletedGroupNum += 1
		}
		// This log would be helpful to check if the deleted groups are all gone.
		if !empty && kgm.getDeletedGroupNum() == 0 {
			log.Info("all the deleted keyspace groups have been cleaned up",
				zap.Uint32("last-deleted-group-id", lastDeletedGroupID),
				zap.Int("last-deleted-group-num", lastDeletedGroupNum))
			// Reset the state to make sure the log won't be printed again
			// until we have new deleted groups.
			empty = true
			lastDeletedGroupNum = 0
		}
	}
}
