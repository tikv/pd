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
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/memberutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	// primaryElectionSuffix is the suffix of the key for keyspace group primary election
	primaryElectionSuffix = "primary"
	// defaultLoadKeyspaceGroupsTimeout is the default timeout for loading the initial
	// keyspace group assignment
	defaultLoadKeyspaceGroupsTimeout   = 30 * time.Second
	defaultLoadKeyspaceGroupsBatchSize = int64(400)
	defaultLoadFromEtcdRetryInterval   = 500 * time.Millisecond
	defaultLoadFromEtcdMaxRetryTimes   = int(defaultLoadKeyspaceGroupsTimeout / defaultLoadFromEtcdRetryInterval)
	watchKEtcdChangeRetryInterval      = 1 * time.Second
)

// KeyspaceGroupManager manages the members of the keyspace groups assigned to this host.
// The replicas campaign for the leaders which provide the tso service for the corresponding
// keyspace groups.
type KeyspaceGroupManager struct {
	// ams stores the allocator managers of the keyspace groups. Each keyspace group is
	// assigned with an allocator manager managing its global/local tso allocators.
	// Use a fixed size array to maximize the efficiency of concurrent access to
	// different keyspace groups for tso service.
	ams [mcsutils.MaxKeyspaceGroupCountInUse]atomic.Pointer[AllocatorManager]
	// kgs stores the keyspace groups' membership/distribution meta.
	kgs [mcsutils.MaxKeyspaceGroupCountInUse]atomic.Pointer[endpoint.KeyspaceGroup]
	// keyspaceLookupTable is a map from keyspace (id) to its keyspace group (id).
	// stored as map[uint32]uint32
	keyspaceLookupTable sync.Map

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// tsoServiceID is the service ID of the TSO service, registered in the service discovery
	tsoServiceID *discovery.ServiceRegistryEntry
	etcdClient   *clientv3.Client
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
}

// NewKeyspaceGroupManager creates a new Keyspace Group Manager.
func NewKeyspaceGroupManager(
	ctx context.Context,
	tsoServiceID *discovery.ServiceRegistryEntry,
	etcdClient *clientv3.Client,
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
		ctx:                         ctx,
		cancel:                      cancel,
		tsoServiceID:                tsoServiceID,
		etcdClient:                  etcdClient,
		electionNamePrefix:          electionNamePrefix,
		legacySvcRootPath:           legacySvcRootPath,
		tsoSvcRootPath:              tsoSvcRootPath,
		cfg:                         cfg,
		loadKeyspaceGroupsTimeout:   defaultLoadKeyspaceGroupsTimeout,
		loadKeyspaceGroupsBatchSize: defaultLoadKeyspaceGroupsBatchSize,
		loadFromEtcdMaxRetryTimes:   defaultLoadFromEtcdMaxRetryTimes,
	}

	kgm.legacySvcStorage = endpoint.NewStorageEndpoint(
		kv.NewEtcdKVBase(kgm.etcdClient, kgm.legacySvcRootPath), nil)
	kgm.tsoSvcStorage = endpoint.NewStorageEndpoint(
		kv.NewEtcdKVBase(kgm.etcdClient, kgm.tsoSvcRootPath), nil)
	return kgm
}

// Initialize this KeyspaceGroupManager
func (kgm *KeyspaceGroupManager) Initialize(loadFromStorage bool) error {
	// Initialize the default keyspace group if not loading from storage
	if !loadFromStorage {
		group := &endpoint.KeyspaceGroup{
			ID:        mcsutils.DefaultKeySpaceGroupID,
			Members:   []endpoint.KeyspaceGroupMember{{Address: kgm.tsoServiceID.ServiceAddr}},
			Keyspaces: []uint32{mcsutils.DefaultKeyspaceID},
		}
		kgm.updateKeyspaceGroup(group)
		return nil
	}

	// Load the initial keyspace group assignment from storage with time limit
	done := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(kgm.ctx)
	go kgm.checkInitProgress(ctx, cancel, done)
	watchStartRevision, err := kgm.initAssignment(ctx)
	done <- struct{}{}
	if err != nil {
		log.Error("failed to initialize keyspace group manager", errs.ZapError(err))
		// We might have partially loaded/initialized the keyspace groups. Close the manager to clean up.
		kgm.Close()
		return err
	}

	// Watch/apply keyspace group membership/distribution meta changes dynamically.
	kgm.wg.Add(1)
	go kgm.startKeyspaceGroupsMetaWatchLoop(watchStartRevision)

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
	kgm.closeKeyspaceGroups()

	log.Info("keyspace group manager closed")
}

func (kgm *KeyspaceGroupManager) closeKeyspaceGroups() {
	log.Info("closing all keyspace groups")

	wg := sync.WaitGroup{}
	for i := range kgm.ams {
		if am := kgm.ams[i].Load(); am != nil {
			wg.Add(1)
			go func(am *AllocatorManager) {
				defer wg.Done()
				am.close()
				log.Info("keyspace group closed", zap.Uint32("keyspace-group-id", am.ksgID))
			}(am)
		}
	}
	wg.Wait()

	log.Info("All keyspace groups closed")
}

func (kgm *KeyspaceGroupManager) checkInitProgress(ctx context.Context, cancel context.CancelFunc, done chan struct{}) {
	select {
	case <-done:
		return
	case <-time.After(kgm.loadKeyspaceGroupsTimeout):
		log.Error("failed to initialize keyspace group manager",
			zap.Any("timeout-setting", kgm.loadKeyspaceGroupsTimeout),
			errs.ZapError(errs.ErrLoadKeyspaceGroupsTimeout))
		cancel()
	case <-ctx.Done():
	}
	<-done
}

// initAssignment loads initial keyspace group assignment from storage and initialize the group manager.
func (kgm *KeyspaceGroupManager) initAssignment(ctx context.Context) (int64, error) {
	var (
		// The start revision for watching keyspace group membership/distribution change
		watchStartRevision   int64
		groups               []*endpoint.KeyspaceGroup
		more                 bool
		err                  error
		keyspaceGroupsLoaded uint32
		revision             int64
	)

	// Load all keyspace groups from etcd and apply the ones assigned to this tso service.
	for {
		revision, groups, more, err = kgm.loadKeyspaceGroups(ctx, keyspaceGroupsLoaded, kgm.loadKeyspaceGroupsBatchSize)
		if err != nil {
			return 0, err
		}

		keyspaceGroupsLoaded += uint32(len(groups))

		if watchStartRevision == 0 || revision < watchStartRevision {
			watchStartRevision = revision
		}

		// Update the keyspace groups
		for _, group := range groups {
			select {
			case <-ctx.Done():
				return watchStartRevision, errs.ErrLoadKeyspaceGroupsTerminated
			default:
			}

			kgm.updateKeyspaceGroup(group)
		}

		if !more {
			break
		}
	}

	log.Info("loaded keyspace groups", zap.Uint32("keyspace-groups-loaded", keyspaceGroupsLoaded))
	return watchStartRevision, nil
}

// loadKeyspaceGroups loads keyspace groups from the start ID with limit.
// If limit is 0, it will load all keyspace groups from the start ID.
func (kgm *KeyspaceGroupManager) loadKeyspaceGroups(
	ctx context.Context, startID uint32, limit int64,
) (revison int64, ksgs []*endpoint.KeyspaceGroup, more bool, err error) {
	rootPath := kgm.legacySvcRootPath
	startKey := strings.Join([]string{rootPath, endpoint.KeyspaceGroupIDPath(startID)}, "/")
	endKey := strings.Join(
		[]string{rootPath, clientv3.GetPrefixRangeEnd(endpoint.KeyspaceGroupIDPrefix())}, "/")
	opOption := []clientv3.OpOption{clientv3.WithRange(endKey), clientv3.WithLimit(limit)}

	var (
		i    int
		resp *clientv3.GetResponse
	)
	for ; i < kgm.loadFromEtcdMaxRetryTimes; i++ {
		resp, err = etcdutil.EtcdKVGet(kgm.etcdClient, startKey, opOption...)

		failpoint.Inject("delayLoadKeyspaceGroups", func(val failpoint.Value) {
			if sleepIntervalSeconds, ok := val.(int); ok && sleepIntervalSeconds > 0 {
				time.Sleep(time.Duration(sleepIntervalSeconds) * time.Second)
			}
		})

		failpoint.Inject("loadKeyspaceGroupsTemporaryFail", func(val failpoint.Value) {
			if maxFailTimes, ok := val.(int); ok && i < maxFailTimes {
				err = errors.New("fail to read from etcd")
				failpoint.Continue()
			}
		})

		if err == nil && resp != nil {
			break
		}

		select {
		case <-ctx.Done():
			return 0, []*endpoint.KeyspaceGroup{}, false, errs.ErrLoadKeyspaceGroupsTerminated
		case <-time.After(defaultLoadFromEtcdRetryInterval):
		}
	}

	if i == kgm.loadFromEtcdMaxRetryTimes {
		return 0, []*endpoint.KeyspaceGroup{}, false, errs.ErrLoadKeyspaceGroupsRetryExhaustd.FastGenByArgs(err)
	}

	kgs := make([]*endpoint.KeyspaceGroup, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		kg := &endpoint.KeyspaceGroup{}
		if err = json.Unmarshal(item.Value, kg); err != nil {
			return 0, nil, false, err
		}
		kgs = append(kgs, kg)
	}

	if resp.Header != nil {
		revison = resp.Header.Revision
	}

	return revison, kgs, resp.More, nil
}

// startKeyspaceGroupsMetaWatchLoop Repeatedly watches any change in keyspace group membership/distribution
// and apply the change dynamically.
func (kgm *KeyspaceGroupManager) startKeyspaceGroupsMetaWatchLoop(revision int64) {
	defer logutil.LogPanic()
	defer kgm.wg.Done()

	// Repeatedly watch/apply keyspace group membership/distribution changes until the context is canceled.
	for {
		select {
		case <-kgm.ctx.Done():
			return
		default:
		}

		nextRevision, err := kgm.watchKeyspaceGroupsMetaChange(revision)
		if err != nil {
			log.Error("watcher canceled unexpectedly. Will start a new watcher after a while",
				zap.Int64("next-revision", nextRevision),
				zap.Time("retry-at", time.Now().Add(watchKEtcdChangeRetryInterval)),
				zap.Error(err))
			time.Sleep(watchKEtcdChangeRetryInterval)
		}
	}
}

// watchKeyspaceGroupsMetaChange watches any change in keyspace group membership/distribution
// and apply the change dynamically.
func (kgm *KeyspaceGroupManager) watchKeyspaceGroupsMetaChange(revision int64) (int64, error) {
	watcher := clientv3.NewWatcher(kgm.etcdClient)
	defer watcher.Close()

	ksgPrefix := strings.Join([]string{kgm.legacySvcRootPath, endpoint.KeyspaceGroupIDPrefix()}, "/")

	for {
		watchChan := watcher.Watch(kgm.ctx, ksgPrefix, clientv3.WithPrefix(), clientv3.WithRev(revision))
		for wresp := range watchChan {
			if wresp.CompactRevision != 0 {
				log.Warn("Required revision has been compacted, the watcher will watch again with the compact revision",
					zap.Int64("required-revision", revision),
					zap.Int64("compact-revision", wresp.CompactRevision))
				revision = wresp.CompactRevision
				break
			}
			if wresp.Err() != nil {
				log.Error("watch is canceled or closed",
					zap.Int64("required-revision", revision),
					errs.ZapError(errs.ErrEtcdWatcherCancel, wresp.Err()))
				return revision, wresp.Err()
			}
			for _, event := range wresp.Events {
				id, err := endpoint.ExtractKeyspaceGroupIDFromPath(string(event.Kv.Key))
				if err != nil {
					log.Warn("failed to extract keyspace group ID from the key path",
						zap.String("key-path", string(event.Kv.Key)), zap.Error(err))
					continue
				}

				switch event.Type {
				case clientv3.EventTypePut:
					group := &endpoint.KeyspaceGroup{}
					if err := json.Unmarshal(event.Kv.Value, group); err != nil {
						log.Warn("failed to unmarshal keyspace group",
							zap.Uint32("keysapce-group-id", id),
							zap.Error(errs.ErrJSONUnmarshal.Wrap(err).FastGenWithCause()))
					} else {
						kgm.updateKeyspaceGroup(group)
					}
				case clientv3.EventTypeDelete:
					kgm.deleteKeyspaceGroup(id)
				}
			}
			revision = wresp.Header.Revision
		}

		select {
		case <-kgm.ctx.Done():
			return revision, nil
		default:
		}
	}
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
	if group.ID >= uint32(len(kgm.ams)) {
		log.Warn("keyspace group ID is out of range, ignore it",
			zap.Uint32("keyspace-group-id", group.ID), zap.Int("max-keyspace-group-id", len(kgm.ams)-1))
		return
	}

	assignedToMe := kgm.isAssignedToMe(group)
	if assignedToMe {
		if kgm.ams[group.ID].Load() != nil {
			log.Info("keyspace group already initialized, so update meta only",
				zap.Uint32("keyspace-group-id", group.ID))

			oldGroup := kgm.kgs[group.ID].Load()
			group.KeyspaceLookupTable = kgm.updateKeyspaceGroupMembership(
				group.ID, oldGroup.Keyspaces, group.Keyspaces, oldGroup.KeyspaceLookupTable)
			kgm.kgs[group.ID].Store(group)
			return
		}

		uniqueName := fmt.Sprintf("%s-%05d", kgm.electionNamePrefix, group.ID)
		uniqueID := memberutil.GenerateUniqueID(uniqueName)
		log.Info("joining primary election",
			zap.Uint32("keyspace-group-id", group.ID),
			zap.String("participant-name", uniqueName),
			zap.Uint64("participant-id", uniqueID))

		participant := member.NewParticipant(kgm.etcdClient)
		participant.InitInfo(
			uniqueName, uniqueID, path.Join(kgm.tsoSvcRootPath, fmt.Sprintf("%05d", group.ID)),
			primaryElectionSuffix, "keyspace group primary election", kgm.cfg.GetAdvertiseListenAddr())

		// Only the default keyspace group uses the legacy service root path for LoadTimestamp/SyncTimestamp.
		var (
			tsRootPath string
			storage    *endpoint.StorageEndpoint
		)
		if group.ID == mcsutils.DefaultKeySpaceGroupID {
			tsRootPath = kgm.legacySvcRootPath
			storage = kgm.legacySvcStorage
		} else {
			tsRootPath = kgm.tsoSvcRootPath
			storage = kgm.tsoSvcStorage
		}

		group.KeyspaceLookupTable = kgm.buildKeyspaceLookupTable(group.ID, group.Keyspaces)
		kgm.kgs[group.ID].Store(group)
		kgm.ams[group.ID].Store(NewAllocatorManager(kgm.ctx, group.ID, participant, tsRootPath, storage, kgm.cfg, true))
	} else {
		// Not assigned to me. If this host/pod owns this keyspace group, it should resign.
		kgm.deleteKeyspaceGroup(group.ID)
	}
}

// updateKeyspaceGroupMembership updates the keyspace lookup table for the given keyspace group.
// Mostly, the membership has no change, so we optimize for this case.
func (kgm *KeyspaceGroupManager) updateKeyspaceGroupMembership(
	groupID uint32, oldKeyspaces, newKeyspaces []uint32,
	defaultKeyspaceLookupTable map[uint32]struct{},
) (keyspaceLookupTable map[uint32]struct{}) {
	oldLen := len(oldKeyspaces)
	newLen := len(newKeyspaces)

	sameMembership := true
	i, j := 0, 0
	for i < oldLen || j < newLen {
		// We assume that the keyspace IDs are sorted in ascending order. If not, we will
		// break the loop and do a full update.
		if i < oldLen && i > 0 && oldKeyspaces[i-1] >= oldKeyspaces[i] {
			break
		}
		if j < newLen && j > 0 && newKeyspaces[j-1] >= newKeyspaces[j] {
			break
		}

		if i < oldLen && j < newLen && oldKeyspaces[i] == newKeyspaces[j] {
			i++
			j++
		} else if i < oldLen && j < newLen && oldKeyspaces[i] < newKeyspaces[j] || j == newLen {
			sameMembership = false
			kgm.keyspaceLookupTable.Delete(oldKeyspaces[i])
			i++
		} else {
			sameMembership = false
			kgm.keyspaceLookupTable.Store(newKeyspaces[j], groupID)
			if keyspaceLookupTable == nil {
				keyspaceLookupTable = make(map[uint32]struct{})
			}
			keyspaceLookupTable[newKeyspaces[j]] = struct{}{}
			j++
		}
	}

	if i < oldLen || j < newLen {
		log.Warn("keyspace IDs are not sorted in ascending order, do a full update",
			zap.Uint32("keyspace-group-id", groupID))

		for ; i < oldLen; i++ {
			kgm.keyspaceLookupTable.Delete(oldKeyspaces[i])
		}
		// Do a full update, because the keyspace IDs are not sorted in ascending order.
		// Sort the keyspaces in ascending order
		sort.Slice(newKeyspaces, func(i, j int) bool {
			return newKeyspaces[i] < newKeyspaces[j]
		})
		keyspaceLookupTable = kgm.buildKeyspaceLookupTable(groupID, newKeyspaces)
	} else if sameMembership {
		// The keyspace group membership is not changed, so we reuse the old one.
		return defaultKeyspaceLookupTable
	} else {
		// The keyspace group membership is changed, so we update the keyspace lookup table.
		// We haven't added the keyspace IDs which belong to both old and new groups, so add them.
		if keyspaceLookupTable == nil {
			keyspaceLookupTable = make(map[uint32]struct{})
		}
		for i, j = 0, 0; i < oldLen && j < newLen; {
			if oldKeyspaces[i] == newKeyspaces[j] {
				keyspaceLookupTable[oldKeyspaces[i]] = struct{}{}
				i++
				j++
			} else if oldKeyspaces[i] < newKeyspaces[j] {
				i++
			} else {
				j++
			}
		}
	}

	return keyspaceLookupTable
}

func (kgm *KeyspaceGroupManager) buildKeyspaceLookupTable(groupID uint32, keyspaces []uint32) map[uint32]struct{} {
	keyspaceLookupTable := make(map[uint32]struct{})
	for _, kid := range keyspaces {
		keyspaceLookupTable[kid] = struct{}{}
		kgm.keyspaceLookupTable.Store(kid, groupID)
	}
	return keyspaceLookupTable
}

// deleteKeyspaceGroup deletes the given keyspace group.
func (kgm *KeyspaceGroupManager) deleteKeyspaceGroup(groupID uint32) {
	kg := kgm.kgs[groupID].Swap(nil)
	if kg != nil {
		for _, kid := range kg.Keyspaces {
			kgm.keyspaceLookupTable.CompareAndDelete(kid, kg.ID)
		}
	}
	am := kgm.ams[groupID].Swap(nil)
	if am != nil {
		am.close()
	}

	log.Info("deleted keyspace group", zap.Uint32("keyspace-group-id", groupID))
}

// GetAllocatorManager returns the AllocatorManager of the given keyspace group
func (kgm *KeyspaceGroupManager) GetAllocatorManager(keyspaceGroupID uint32) (*AllocatorManager, error) {
	if err := kgm.checkKeySpaceGroupID(keyspaceGroupID); err != nil {
		return nil, err
	}
	if am := kgm.ams[keyspaceGroupID].Load(); am != nil {
		return am, nil
	}
	return nil, kgm.genNotServedErr(errs.ErrGetAllocatorManager, keyspaceGroupID)
}

// GetAMWithMembershipCheck returns the AllocatorManager of the given keyspace group and check if the keyspace
// is served by this keyspace group.
func (kgm *KeyspaceGroupManager) GetAMWithMembershipCheck(
	keyspaceID, keyspaceGroupID uint32,
) (*AllocatorManager, error) {
	if am := kgm.ams[keyspaceGroupID].Load(); am != nil {
		ksg := kgm.kgs[keyspaceGroupID].Load()
		if ksg == nil {
			return nil, kgm.genNotServedErr(errs.ErrGetAllocatorManager, keyspaceGroupID)
		}
		if _, ok := ksg.KeyspaceLookupTable[keyspaceID]; !ok {
			return nil, kgm.genNotServedErr(errs.ErrGetAllocatorManager, keyspaceGroupID)
		}
		return am, nil
	}
	return nil, kgm.genNotServedErr(errs.ErrGetAllocatorManager, keyspaceGroupID)
}

// GetElectionMember returns the election member of the given keyspace group
// TODO: support multiple keyspace groups for GetElectionMember
func (kgm *KeyspaceGroupManager) GetElectionMember(
	keyspaceID, keyspaceGroupID uint32,
) (ElectionMember, error) {
	if err := kgm.checkKeySpaceGroupID(keyspaceGroupID); err != nil {
		return nil, err
	}
	am, err := kgm.GetAMWithMembershipCheck(keyspaceID, keyspaceGroupID)
	if err != nil {
		return nil, err
	}
	return am.getMember(), nil
}

// HandleTSORequest forwards TSO allocation requests to correct TSO Allocators of the given keyspace group.
func (kgm *KeyspaceGroupManager) HandleTSORequest(
	keyspaceID, keyspaceGroupID uint32,
	dcLocation string, count uint32,
) (ts pdpb.Timestamp, currentKeyspaceGroupID uint32, err error) {
	if err := kgm.checkKeySpaceGroupID(keyspaceGroupID); err != nil {
		return pdpb.Timestamp{}, keyspaceGroupID, err
	}
	am, err := kgm.GetAMWithMembershipCheck(keyspaceID, keyspaceGroupID)
	if err != nil {
		// The keyspace doesn't belong to this keyspace group, we should check if it belongs to any other
		// keyspace groups, and return the correct keyspace group ID to the client.
		kgid, loaded := kgm.keyspaceLookupTable.Load(keyspaceID)
		if loaded && kgid != nil {
			return pdpb.Timestamp{}, kgid.(uint32), err
		}
		return pdpb.Timestamp{}, keyspaceGroupID, errs.ErrKeyspaceNotAssigned.FastGenByArgs(keyspaceID)
	}
	ts, err = am.HandleRequest(dcLocation, count)
	return ts, keyspaceGroupID, err
}

func (kgm *KeyspaceGroupManager) checkKeySpaceGroupID(id uint32) error {
	if id < mcsutils.MaxKeyspaceGroupCountInUse {
		return nil
	}
	return errs.ErrKeyspaceGroupIDInvalid.FastGenByArgs(
		fmt.Sprintf("%d which shouldn't >= %d", id, mcsutils.MaxKeyspaceGroupCountInUse))
}

func (kgm *KeyspaceGroupManager) genNotServedErr(perr *perrors.Error, keyspaceGroupID uint32) error {
	return perr.FastGenByArgs(
		fmt.Sprintf(
			"requested keyspace group with id %d %s by this host/pod",
			keyspaceGroupID, errs.NotServedErr))
}
