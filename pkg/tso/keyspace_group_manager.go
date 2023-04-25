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
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
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
	watchEtcdChangeRetryInterval       = 1 * time.Second
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

// getAMWithMembershipCheck returns the AllocatorManager of the given keyspace group and check
// if the keyspace is served by this keyspace group.
func (s *state) getAMWithMembershipCheck(
	keyspaceID, keyspaceGroupID uint32,
) (*AllocatorManager, uint32, error) {
	s.RLock()
	defer s.RUnlock()

	if am := s.ams[keyspaceGroupID]; am != nil {
		kg := s.kgs[keyspaceGroupID]
		if kg != nil {
			if _, ok := kg.KeyspaceLookupTable[keyspaceID]; ok {
				return am, keyspaceGroupID, nil
			}
		}
	}

	// The keyspace doesn't belong to this keyspace group, we should check if it belongs to any other
	// keyspace groups, and return the correct keyspace group ID to the client.
	if kgid, ok := s.keyspaceLookupTable[keyspaceID]; ok {
		return nil, kgid, genNotServedErr(errs.ErrGetAllocatorManager, keyspaceGroupID)
	}

	if keyspaceGroupID != mcsutils.DefaultKeyspaceGroupID {
		return nil, keyspaceGroupID, errs.ErrKeyspaceNotAssigned.FastGenByArgs(keyspaceID)
	}

	// The keyspace doesn't belong to any keyspace group, so return the default keyspace group.
	// It's for migrating the existing keyspaces which have no keyspace group assigned, so the
	// the default keyspace group is used to serve the keyspaces.
	return s.ams[mcsutils.DefaultKeyspaceGroupID], mcsutils.DefaultKeyspaceGroupID, nil
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
		ctx:                         ctx,
		cancel:                      cancel,
		tsoServiceID:                tsoServiceID,
		etcdClient:                  etcdClient,
		httpClient:                  httpClient,
		electionNamePrefix:          electionNamePrefix,
		legacySvcRootPath:           legacySvcRootPath,
		tsoSvcRootPath:              tsoSvcRootPath,
		cfg:                         cfg,
		loadKeyspaceGroupsTimeout:   defaultLoadKeyspaceGroupsTimeout,
		loadKeyspaceGroupsBatchSize: defaultLoadKeyspaceGroupsBatchSize,
		loadFromEtcdMaxRetryTimes:   defaultLoadFromEtcdMaxRetryTimes,
		groupUpdateRetryList:        make(map[uint32]*endpoint.KeyspaceGroup),
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
	// Load the initial keyspace group assignment from storage with time limit
	done := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(kgm.ctx)
	go kgm.checkInitProgress(ctx, cancel, done)
	watchStartRevision, defaultKGConfigured, err := kgm.initAssignment(ctx)
	done <- struct{}{}
	if err != nil {
		log.Error("failed to initialize keyspace group manager", errs.ZapError(err))
		// We might have partially loaded/initialized the keyspace groups. Close the manager to clean up.
		kgm.Close()
		return err
	}

	// Initialize the default keyspace group if it isn't configured in the storage.
	if !defaultKGConfigured {
		keyspaces := []uint32{mcsutils.DefaultKeyspaceID}
		kgm.initDefaultKeyspaceGroup(keyspaces)
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
	kgm.state.deinitialize()

	log.Info("keyspace group manager closed")
}

func (kgm *KeyspaceGroupManager) checkInitProgress(ctx context.Context, cancel context.CancelFunc, done chan struct{}) {
	defer logutil.LogPanic()

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

func (kgm *KeyspaceGroupManager) initDefaultKeyspaceGroup(keyspaces []uint32) {
	log.Info("initializing default keyspace group",
		zap.Int("keyspaces-length", len(keyspaces)))

	group := &endpoint.KeyspaceGroup{
		ID:        mcsutils.DefaultKeyspaceGroupID,
		Members:   []endpoint.KeyspaceGroupMember{{Address: kgm.tsoServiceID.ServiceAddr}},
		Keyspaces: keyspaces,
	}
	kgm.updateKeyspaceGroup(group)
}

// initAssignment loads initial keyspace group assignment from storage and initialize the group manager.
// Return watchStartRevision, the start revision for watching keyspace group membership/distribution change.
func (kgm *KeyspaceGroupManager) initAssignment(
	ctx context.Context,
) (watchStartRevision int64, defaultKGConfigured bool, err error) {
	var (
		groups               []*endpoint.KeyspaceGroup
		more                 bool
		keyspaceGroupsLoaded uint32
		revision             int64
	)

	// Load all keyspace groups from etcd and apply the ones assigned to this tso service.
	for {
		revision, groups, more, err = kgm.loadKeyspaceGroups(ctx, keyspaceGroupsLoaded, kgm.loadKeyspaceGroupsBatchSize)
		if err != nil {
			return
		}

		keyspaceGroupsLoaded += uint32(len(groups))

		if watchStartRevision == 0 || revision < watchStartRevision {
			watchStartRevision = revision
		}

		// Update the keyspace groups
		for _, group := range groups {
			select {
			case <-ctx.Done():
				err = errs.ErrLoadKeyspaceGroupsTerminated
				return
			default:
			}

			if group.ID == mcsutils.DefaultKeyspaceGroupID {
				defaultKGConfigured = true
			}

			kgm.updateKeyspaceGroup(group)
		}

		if !more {
			break
		}
	}

	log.Info("loaded keyspace groups", zap.Uint32("keyspace-groups-loaded", keyspaceGroupsLoaded))
	return
}

// loadKeyspaceGroups loads keyspace groups from the start ID with limit.
// If limit is 0, it will load all keyspace groups from the start ID.
func (kgm *KeyspaceGroupManager) loadKeyspaceGroups(
	ctx context.Context, startID uint32, limit int64,
) (revision int64, ksgs []*endpoint.KeyspaceGroup, more bool, err error) {
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
		return 0, []*endpoint.KeyspaceGroup{}, false, errs.ErrLoadKeyspaceGroupsRetryExhausted.FastGenByArgs(err)
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
		revision = resp.Header.Revision + 1
	}

	return revision, kgs, resp.More, nil
}

// startKeyspaceGroupsMetaWatchLoop repeatedly watches any change in keyspace group membership/distribution
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
			log.Error("watcher canceled unexpectedly and a new watcher will start after a while",
				zap.Int64("next-revision", nextRevision),
				zap.Time("retry-at", time.Now().Add(watchEtcdChangeRetryInterval)),
				zap.Error(err))
			time.Sleep(watchEtcdChangeRetryInterval)
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
				groupID, err := endpoint.ExtractKeyspaceGroupIDFromPath(string(event.Kv.Key))
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
							zap.Uint32("keyspace-group-id", groupID),
							zap.Error(errs.ErrJSONUnmarshal.Wrap(err).FastGenWithCause()))
						break
					}
					kgm.updateKeyspaceGroup(group)
				case clientv3.EventTypeDelete:
					if groupID == mcsutils.DefaultKeyspaceGroupID {
						keyspaces := kgm.kgs[groupID].Keyspaces
						kgm.deleteKeyspaceGroup(groupID)
						log.Warn("removed default keyspace group meta config from the storage. " +
							"now every tso node/pod will initialize it")
						kgm.initDefaultKeyspaceGroup(keyspaces)
					} else {
						kgm.deleteKeyspaceGroup(groupID)
					}
				}
			}
			// Retry the groups that are not initialized successfully before.
			for id, group := range kgm.groupUpdateRetryList {
				delete(kgm.groupUpdateRetryList, id)
				kgm.updateKeyspaceGroup(group)
			}
			revision = wresp.Header.Revision + 1
		}

		select {
		case <-kgm.ctx.Done():
			return revision, nil
		default:
		}
	}
}

func (kgm *KeyspaceGroupManager) isAssignedToMe(group *endpoint.KeyspaceGroup) bool {
	// If the default keyspace group isn't assigned to any tso node/pod, assign it to everyone.
	if group.ID == mcsutils.DefaultKeyspaceGroupID && len(group.Members) == 0 {
		return true
	}

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
	// Not assigned to me. If this host/pod owns this keyspace group, it should resign.
	if !kgm.isAssignedToMe(group) {
		if group.ID == mcsutils.DefaultKeyspaceGroupID {
			log.Info("resign default keyspace group membership",
				zap.Any("default-keyspace-group", group))
		}
		kgm.deleteKeyspaceGroup(group.ID)
		return
	}
	// If the keyspace group is already initialized, just update the meta.
	if oldAM, oldGroup := kgm.state.getKeyspaceGroupMeta(group.ID); oldAM != nil {
		log.Info("keyspace group already initialized, so update meta only",
			zap.Uint32("keyspace-group-id", group.ID))
		kgm.updateKeyspaceGroupMembership(oldGroup, group)
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
		splitSourceAM, _ := kgm.getKeyspaceGroupMeta(splitSource)
		if splitSourceAM == nil {
			log.Error("the split source keyspace group is not initialized",
				zap.Uint32("target", group.ID),
				zap.Uint32("source", splitSource))
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

// updateKeyspaceGroupMembership updates the keyspace lookup table for the given keyspace group.
func (kgm *KeyspaceGroupManager) updateKeyspaceGroupMembership(
	oldGroup, newGroup *endpoint.KeyspaceGroup,
) {
	groupID := newGroup.ID
	oldKeyspaces, newKeyspaces := oldGroup.Keyspaces, newGroup.Keyspaces
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

	kgm.Lock()
	defer kgm.Unlock()

	if sameMembership {
		// The keyspace group membership is not changed. Reuse the old one.
		newGroup.KeyspaceLookupTable = oldGroup.KeyspaceLookupTable
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
	if oldGroup.IsSplitTarget() && !newGroup.IsSplitting() {
		kgm.ams[groupID].GetMember().(*member.Participant).SetCampaignChecker(nil)
	}
	kgm.kgs[groupID] = newGroup
}

// deleteKeyspaceGroup deletes the given keyspace group.
func (kgm *KeyspaceGroupManager) deleteKeyspaceGroup(groupID uint32) {
	log.Info("delete keyspace group", zap.Uint32("keyspace-group-id", groupID))

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

// GetElectionMember returns the election member of the given keyspace group
func (kgm *KeyspaceGroupManager) GetElectionMember(
	keyspaceID, keyspaceGroupID uint32,
) (ElectionMember, error) {
	if err := kgm.checkKeySpaceGroupID(keyspaceGroupID); err != nil {
		return nil, err
	}
	am, _, err := kgm.getAMWithMembershipCheck(keyspaceID, keyspaceGroupID)
	if err != nil {
		return nil, err
	}
	return am.GetMember(), nil
}

// HandleTSORequest forwards TSO allocation requests to correct TSO Allocators of the given keyspace group.
func (kgm *KeyspaceGroupManager) HandleTSORequest(
	keyspaceID, keyspaceGroupID uint32,
	dcLocation string, count uint32,
) (ts pdpb.Timestamp, currentKeyspaceGroupID uint32, err error) {
	if err := kgm.checkKeySpaceGroupID(keyspaceGroupID); err != nil {
		return pdpb.Timestamp{}, keyspaceGroupID, err
	}
	am, currentKeyspaceGroupID, err := kgm.getAMWithMembershipCheck(keyspaceID, keyspaceGroupID)
	if err != nil {
		return pdpb.Timestamp{}, currentKeyspaceGroupID, err
	}
	err = kgm.checkTSOSplit(currentKeyspaceGroupID, dcLocation)
	if err != nil {
		return pdpb.Timestamp{}, currentKeyspaceGroupID, err
	}
	ts, err = am.HandleRequest(dcLocation, count)
	return ts, keyspaceGroupID, err
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
