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
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/memberutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

const (
	// primaryElectionSuffix is the suffix of the key for keyspace group primary election
	primaryElectionSuffix = "primary"
	defaultRetryInterval  = 500 * time.Millisecond
)

type state struct {
	sync.RWMutex
	// ams stores the allocator managers of the keyspace groups. Each keyspace group is
	// assigned with an allocator manager managing its global/local tso allocators.
	// Use a fixed size array to maximize the efficiency of concurrent access to
	// different keyspace groups for tso service.
	ams [mcsutils.MaxKeyspaceGroupCountInUse]*AllocatorManager
	// kgs stores the keyspace groups' membership/distribution meta.
	kgs [mcsutils.MaxKeyspaceGroupCountInUse]*endpoint.KeyspaceGroup
	// keyspaceLookupTable is a map from keyspace to the keyspace group to which it belongs.
	keyspaceLookupTable map[uint32]uint32
}

func (s *state) initialize() {
	s.keyspaceLookupTable = make(map[uint32]uint32)
}

func (s *state) deinitialize() {
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
	if keyspaceGroupID != mcsutils.DefaultKeyspaceGroupID {
		return nil, nil, keyspaceGroupID, errs.ErrKeyspaceNotAssigned.FastGenByArgs(keyspaceID)
	}

	// For migrating the existing keyspaces which have no keyspace group assigned as configured
	// in the keyspace meta. All these keyspaces will be served by the default keyspace group.
	if s.ams[mcsutils.DefaultKeyspaceGroupID] == nil {
		return nil, nil, mcsutils.DefaultKeyspaceGroupID,
			errs.ErrKeyspaceNotAssigned.FastGenByArgs(keyspaceID)
	}
	return s.ams[mcsutils.DefaultKeyspaceGroupID],
		s.kgs[mcsutils.DefaultKeyspaceGroupID],
		mcsutils.DefaultKeyspaceGroupID, nil
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
	// legacySvcRootPath defines the legacy root path for all etcd paths which derives from
	// the PD/API service. It's in the format of "/pd/{cluster_id}".
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
	// 1. The path for keyspace group primary election. Format: "/ms/{cluster_id}/tso/{group}/primary"
	// 2. The path for LoadTimestamp/SaveTimestamp in the storage endpoint for all the non-default
	//    keyspace groups.
	//    Key: /ms/{cluster_id}/tso/{group}/gts/timestamp
	//    Value: ts(time.Time)
	//    Key: /ms/{cluster_id}/tso/{group}/lts/{dc-location}/timestamp
	//    Value: ts(time.Time)
	// Note: The {group} is 5 digits integer with leading zeros.
	tsoSvcRootPath string
	// legacySvcStorage is storage with legacySvcRootPath.
	legacySvcStorage *endpoint.StorageEndpoint
	// tsoSvcStorage is storage with tsoSvcRootPath.
	tsoSvcStorage *endpoint.StorageEndpoint
	// cfg is the TSO config
	cfg ServiceConfig

	// loadKeyspaceGroupsTimeout is the timeout for loading the initial keyspace group assignment.
	loadKeyspaceGroupsTimeout   time.Duration
	loadKeyspaceGroupsBatchSize int64
	loadFromEtcdMaxRetryTimes   int

	// groupUpdateRetryList is the list of keyspace groups which failed to update and need to retry.
	groupUpdateRetryList map[uint32]*endpoint.KeyspaceGroup

	groupWatcher *etcdutil.LoopWatcher
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
	if mcsutils.MaxKeyspaceGroupCountInUse > mcsutils.MaxKeyspaceGroupCount {
		log.Fatal("MaxKeyspaceGroupCountInUse is larger than MaxKeyspaceGroupCount",
			zap.Uint32("max-keyspace-group-count-in-use", mcsutils.MaxKeyspaceGroupCountInUse),
			zap.Uint32("max-keyspace-group-count", mcsutils.MaxKeyspaceGroupCount))
	}

	ctx, cancel := context.WithCancel(ctx)
	kgm := &KeyspaceGroupManager{
		ctx:                  ctx,
		cancel:               cancel,
		tsoServiceID:         tsoServiceID,
		etcdClient:           etcdClient,
		httpClient:           httpClient,
		electionNamePrefix:   electionNamePrefix,
		legacySvcRootPath:    legacySvcRootPath,
		tsoSvcRootPath:       tsoSvcRootPath,
		cfg:                  cfg,
		groupUpdateRetryList: make(map[uint32]*endpoint.KeyspaceGroup),
	}
	kgm.legacySvcStorage = endpoint.NewStorageEndpoint(
		kv.NewEtcdKVBase(kgm.etcdClient, kgm.legacySvcRootPath), nil)
	kgm.tsoSvcStorage = endpoint.NewStorageEndpoint(
		kv.NewEtcdKVBase(kgm.etcdClient, kgm.tsoSvcRootPath), nil)
	kgm.state.initialize()
	return kgm
}

// Initialize this KeyspaceGroupManager
func (kgm *KeyspaceGroupManager) Initialize() error {
	rootPath := kgm.legacySvcRootPath
	startKey := strings.Join([]string{rootPath, endpoint.KeyspaceGroupIDPath(mcsutils.DefaultKeyspaceGroupID)}, "/")
	endKey := strings.Join(
		[]string{rootPath, clientv3.GetPrefixRangeEnd(endpoint.KeyspaceGroupIDPrefix())}, "/")

	defaultKGConfigured := false
	putFn := func(kv *mvccpb.KeyValue) error {
		group := &endpoint.KeyspaceGroup{}
		if err := json.Unmarshal(kv.Value, group); err != nil {
			return errs.ErrJSONUnmarshal.Wrap(err).FastGenWithCause()
		}
		kgm.updateKeyspaceGroup(group)
		if group.ID == mcsutils.DefaultKeyspaceGroupID {
			defaultKGConfigured = true
		}
		return nil
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		groupID, err := endpoint.ExtractKeyspaceGroupIDFromPath(string(kv.Key))
		if err != nil {
			return err
		}
		kgm.deleteKeyspaceGroup(groupID)
		return nil
	}
	postEventFn := func() error {
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
		putFn,
		deleteFn,
		postEventFn,
		clientv3.WithRange(endKey),
	)
	if kgm.loadKeyspaceGroupsTimeout > 0 {
		kgm.groupWatcher.SetLoadTimeout(kgm.loadKeyspaceGroupsTimeout)
	}
	if kgm.loadFromEtcdMaxRetryTimes > 0 {
		kgm.groupWatcher.SetLoadRetryTimes(kgm.loadFromEtcdMaxRetryTimes)
	}
	if kgm.loadKeyspaceGroupsBatchSize > 0 {
		kgm.groupWatcher.SetLoadBatchSize(kgm.loadKeyspaceGroupsBatchSize)
	}
	kgm.wg.Add(1)
	go kgm.groupWatcher.StartWatchLoop()

	if err := kgm.groupWatcher.WaitLoad(); err != nil {
		log.Error("failed to initialize keyspace group manager", errs.ZapError(err))
		// We might have partially loaded/initialized the keyspace groups. Close the manager to clean up.
		kgm.Close()
		return errs.ErrLoadKeyspaceGroupsTerminated
	}

	if !defaultKGConfigured {
		log.Info("initializing default keyspace group")
		group := &endpoint.KeyspaceGroup{
			ID:        mcsutils.DefaultKeyspaceGroupID,
			Members:   []endpoint.KeyspaceGroupMember{{Address: kgm.tsoServiceID.ServiceAddr}},
			Keyspaces: []uint32{mcsutils.DefaultKeyspaceID},
		}
		kgm.updateKeyspaceGroup(group)
	}
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
	kgm.state.deinitialize()

	log.Info("keyspace group manager closed")
}

func (kgm *KeyspaceGroupManager) isAssignedToMe(group *endpoint.KeyspaceGroup) bool {
	for _, member := range group.Members {
		if member.Address == kgm.tsoServiceID.ServiceAddr {
			return true
		}
	}
	return false
}

// updateKeyspaceGroup applies the given keyspace group. If the keyspace group is just assigned to
// this host/pod, it will join the primary election.
func (kgm *KeyspaceGroupManager) updateKeyspaceGroup(group *endpoint.KeyspaceGroup) {
	if err := kgm.checkKeySpaceGroupID(group.ID); err != nil {
		log.Warn("keyspace group ID is invalid, ignore it", zap.Error(err))
		return
	}

	// If the default keyspace group isn't assigned to any tso node/pod, assign it to everyone.
	if group.ID == mcsutils.DefaultKeyspaceGroupID && len(group.Members) == 0 {
		log.Warn("configured the default keyspace group but no members/distribution specified. " +
			"ignore it for now and fallback to the way of every tso node/pod owning a replica")
		// TODO: fill members with all tso nodes/pods.
		group.Members = []endpoint.KeyspaceGroupMember{{Address: kgm.tsoServiceID.ServiceAddr}}
	}

	if !kgm.isAssignedToMe(group) {
		// Not assigned to me. If this host/pod owns a replica of this keyspace group,
		// it should resign the election membership now.
		kgm.exitElectionMembership(group)
		return
	}

	// If this host is already assigned a replica of this keyspace group, that is to is already initialized, just update the meta.
	if oldAM, oldGroup := kgm.getKeyspaceGroupMeta(group.ID); oldAM != nil {
		log.Info("keyspace group already initialized, so update meta only",
			zap.Uint32("keyspace-group-id", group.ID))
		kgm.updateKeyspaceGroupMembership(oldGroup, group, true)
		return
	}

	// If the keyspace group is not initialized, initialize it.
	uniqueName := fmt.Sprintf("%s-%05d", kgm.electionNamePrefix, group.ID)
	uniqueID := memberutil.GenerateUniqueID(uniqueName)
	log.Info("joining primary election",
		zap.Uint32("keyspace-group-id", group.ID),
		zap.String("participant-name", uniqueName),
		zap.Uint64("participant-id", uniqueID))
	// Initialize the participant info to join the primary election.
	participant := member.NewParticipant(kgm.etcdClient)
	participant.InitInfo(
		uniqueName, uniqueID, path.Join(kgm.tsoSvcRootPath, fmt.Sprintf("%05d", group.ID)),
		primaryElectionSuffix, "keyspace group primary election", kgm.cfg.GetAdvertiseListenAddr())
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
		participant.SetCampaignChecker(func(leadership *election.Leadership) bool {
			return splitSourceAM.GetMember().IsLeader()
		})
	}
	// Only the default keyspace group uses the legacy service root path for LoadTimestamp/SyncTimestamp.
	var (
		tsRootPath string
		storage    *endpoint.StorageEndpoint
	)
	if group.ID == mcsutils.DefaultKeyspaceGroupID {
		tsRootPath = kgm.legacySvcRootPath
		storage = kgm.legacySvcStorage
	} else {
		tsRootPath = kgm.tsoSvcRootPath
		storage = kgm.tsoSvcStorage
	}
	// Initialize all kinds of maps.
	am := NewAllocatorManager(kgm.ctx, group.ID, participant, tsRootPath, storage, kgm.cfg, true)
	kgm.Lock()
	group.KeyspaceLookupTable = make(map[uint32]struct{})
	for _, kid := range group.Keyspaces {
		group.KeyspaceLookupTable[kid] = struct{}{}
		kgm.keyspaceLookupTable[kid] = group.ID
	}
	kgm.kgs[group.ID] = group
	kgm.ams[group.ID] = am
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
	if memberCount < mcsutils.KeyspaceGroupDefaultReplicaCount {
		log.Error("the split source keyspace group does not have enough members",
			zap.Uint32("target", targetGroup.ID),
			zap.Uint32("source", splitSourceID),
			zap.Int("member-count", memberCount),
			zap.Int("replica-count", mcsutils.KeyspaceGroupDefaultReplicaCount))
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
		for i := 0; i < oldLen; i++ {
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
		// The keyspace group membership is changed. Update the keyspace lookup table.
		newGroup.KeyspaceLookupTable = make(map[uint32]struct{})
		for i, j := 0, 0; i < oldLen || j < newLen; {
			if i < oldLen && j < newLen && oldKeyspaces[i] == newKeyspaces[j] {
				newGroup.KeyspaceLookupTable[newKeyspaces[j]] = struct{}{}
				i++
				j++
			} else if i < oldLen && j < newLen && oldKeyspaces[i] < newKeyspaces[j] || j == newLen {
				delete(kgm.keyspaceLookupTable, oldKeyspaces[i])
				i++
			} else {
				newGroup.KeyspaceLookupTable[newKeyspaces[j]] = struct{}{}
				kgm.keyspaceLookupTable[newKeyspaces[j]] = groupID
				j++
			}
		}
		if groupID == mcsutils.DefaultKeyspaceGroupID {
			if _, ok := newGroup.KeyspaceLookupTable[mcsutils.DefaultKeyspaceID]; !ok {
				log.Warn("default keyspace is not in default keyspace group. add it back")
				kgm.keyspaceLookupTable[mcsutils.DefaultKeyspaceID] = groupID
				newGroup.KeyspaceLookupTable[mcsutils.DefaultKeyspaceID] = struct{}{}
				newGroup.Keyspaces = make([]uint32, 1+len(newKeyspaces))
				newGroup.Keyspaces[0] = mcsutils.DefaultKeyspaceID
				copy(newGroup.Keyspaces[1:], newKeyspaces)
			}
		} else {
			if _, ok := newGroup.KeyspaceLookupTable[mcsutils.DefaultKeyspaceID]; ok {
				log.Warn("default keyspace is in non-default keyspace group. remove it")
				kgm.keyspaceLookupTable[mcsutils.DefaultKeyspaceID] = mcsutils.DefaultKeyspaceGroupID
				delete(newGroup.KeyspaceLookupTable, mcsutils.DefaultKeyspaceID)
				newGroup.Keyspaces = newKeyspaces[1:]
			}
		}
	}
	// Check if the split is completed.
	if oldGroup != nil && oldGroup.IsSplitTarget() && !newGroup.IsSplitting() {
		kgm.ams[groupID].GetMember().(*member.Participant).SetCampaignChecker(nil)
	}
	kgm.kgs[groupID] = newGroup
}

// deleteKeyspaceGroup deletes the given keyspace group.
func (kgm *KeyspaceGroupManager) deleteKeyspaceGroup(groupID uint32) {
	log.Info("delete keyspace group", zap.Uint32("keyspace-group-id", groupID))

	if groupID == mcsutils.DefaultKeyspaceGroupID {
		log.Info("removed default keyspace group meta config from the storage. " +
			"now every tso node/pod will initialize it")
		group := &endpoint.KeyspaceGroup{
			ID:        mcsutils.DefaultKeyspaceGroupID,
			Members:   []endpoint.KeyspaceGroupMember{{Address: kgm.tsoServiceID.ServiceAddr}},
			Keyspaces: []uint32{mcsutils.DefaultKeyspaceID},
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
			//     which has already declared the ownership of the keyspace.
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
}

// exitElectionMembership exits the election membership of the given keyspace group by
// deinitializing the allocator manager, but still keeps the keyspace group info.
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
	if err := kgm.checkKeySpaceGroupID(keyspaceGroupID); err != nil {
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
		kgm.getKeyspaceGroupMetaWithCheck(keyspaceID, mcsutils.DefaultKeyspaceGroupID)
	if err != nil {
		return nil, nil, curKeyspaceGroupID, err
	}
	return curAM, curKeyspaceGroup, curKeyspaceGroupID, nil
}

// GetElectionMember returns the election member of the keyspace group serving the given keyspace.
func (kgm *KeyspaceGroupManager) GetElectionMember(
	keyspaceID, keyspaceGroupID uint32,
) (ElectionMember, error) {
	if err := kgm.checkKeySpaceGroupID(keyspaceGroupID); err != nil {
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
	keyspaceID, keyspaceGroupID uint32,
	dcLocation string, count uint32,
) (ts pdpb.Timestamp, curKeyspaceGroupID uint32, err error) {
	if err := kgm.checkKeySpaceGroupID(keyspaceGroupID); err != nil {
		return pdpb.Timestamp{}, keyspaceGroupID, err
	}
	am, _, curKeyspaceGroupID, err := kgm.getKeyspaceGroupMetaWithCheck(keyspaceID, keyspaceGroupID)
	if err != nil {
		return pdpb.Timestamp{}, curKeyspaceGroupID, err
	}
	err = kgm.checkTSOSplit(curKeyspaceGroupID, dcLocation)
	if err != nil {
		return pdpb.Timestamp{}, curKeyspaceGroupID, err
	}
	ts, err = am.HandleRequest(dcLocation, count)
	return ts, curKeyspaceGroupID, err
}

func (kgm *KeyspaceGroupManager) checkKeySpaceGroupID(id uint32) error {
	if id < mcsutils.MaxKeyspaceGroupCountInUse {
		return nil
	}
	return errs.ErrKeyspaceGroupIDInvalid.FastGenByArgs(
		fmt.Sprintf("%d shouldn't >= %d", id, mcsutils.MaxKeyspaceGroupCountInUse))
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
	dcLocation string,
) error {
	splitAM, splitGroup := kgm.getKeyspaceGroupMeta(keyspaceGroupID)
	// Only the split target keyspace group needs to check the TSO split.
	if !splitGroup.IsSplitTarget() {
		return nil
	}
	splitSource := splitGroup.SplitSource()
	splitSourceAM, splitSourceGroup := kgm.getKeyspaceGroupMeta(splitSource)
	if splitSourceAM == nil || splitSourceGroup == nil {
		log.Error("the split source keyspace group is not initialized",
			zap.Uint32("source", splitSource))
		return errs.ErrKeyspaceGroupNotInitialized.FastGenByArgs(splitSource)
	}
	splitAllocator, err := splitAM.GetAllocator(dcLocation)
	if err != nil {
		return err
	}
	splitSourceAllocator, err := splitSourceAM.GetAllocator(dcLocation)
	if err != nil {
		return err
	}
	splitTSO, err := splitAllocator.GenerateTSO(1)
	if err != nil {
		return err
	}
	splitSourceTSO, err := splitSourceAllocator.GenerateTSO(1)
	if err != nil {
		return err
	}
	if tsoutil.CompareTimestamp(&splitSourceTSO, &splitTSO) <= 0 {
		return nil
	}
	// If the split source TSO is greater than the newly split TSO, we need to update the split
	// TSO to make sure the following TSO will be greater than the split keyspaces ever had
	// in the past.
	splitSourceTSO.Physical += 1
	err = splitAllocator.SetTSO(tsoutil.GenerateTS(&splitSourceTSO), true, true)
	if err != nil {
		return err
	}
	// Finish the split state.
	return kgm.finishSplitKeyspaceGroup(keyspaceGroupID)
}

const keyspaceGroupsAPIPrefix = "/pd/api/v2/tso/keyspace-groups"

// Put the code below into the critical section to prevent from sending too many HTTP requests.
func (kgm *KeyspaceGroupManager) finishSplitKeyspaceGroup(id uint32) error {
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
	statusCode, err := apiutil.DoDelete(
		kgm.httpClient,
		kgm.cfg.GeBackendEndpoints()+keyspaceGroupsAPIPrefix+fmt.Sprintf("/%d/split", id))
	if err != nil {
		return err
	}
	if statusCode != http.StatusOK {
		log.Warn("failed to finish split keyspace group",
			zap.Uint32("keyspace-group-id", id),
			zap.Int("status-code", statusCode))
		return errs.ErrSendRequest.FastGenByArgs()
	}
	// Pre-update the split keyspace group split state in memory.
	splitGroup.SplitState = nil
	kgm.kgs[id] = splitGroup
	return nil
}
