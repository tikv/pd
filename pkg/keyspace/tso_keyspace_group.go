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

package keyspace

import (
	"bytes"
	"context"
	"encoding/json"
	goerrors "errors"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/balancer"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/mcs/discovery"
	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

const (
	defaultBalancerPolicy              = balancer.PolicyRoundRobin
	allocNodesToKeyspaceGroupsInterval = 1 * time.Second
	allocNodesTimeout                  = 1 * time.Second
	allocNodesInterval                 = 10 * time.Millisecond
	// Each keyspace removal can add two delete operations plus, in the worst
	// case, one meta-service group status update. Together with saving the
	// keyspace group, a full batch uses at most 91 operations, leaving extra
	// headroom below PD's conservative etcd transaction limit of 120.
	maxKeyspaceRemovalBatchSize = 30
	// defaultKeyspaceCountSplitThreshold is the keyspace count threshold for auto-splitting
	// a keyspace group. When a group's keyspace count exceeds this value, a new group will be split automatically.
	defaultKeyspaceCountSplitThreshold = 40000
	// autoSplitKeyspaceGroupPatrolInterval is the interval for patrolling keyspace group size for auto-split.
	autoSplitKeyspaceGroupPatrolInterval = 15 * time.Minute
)

const (
	opAdd int = iota
	opDelete
)

// GroupManager is the manager of keyspace group related data.
type GroupManager struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	client *clientv3.Client

	syncutil.RWMutex
	// membershipMutationLock prevents operations that relocate existing keyspaces
	// from interleaving with a multi-batch removal. Adding a newly created
	// keyspace does not take this lock, so creation can proceed between batches.
	membershipMutationLock syncutil.RWMutex
	// groups is the cache of keyspace group related information.
	// user kind -> keyspace group
	groups map[endpoint.UserKind]*indexedHeap

	// store is the storage for keyspace group related information.
	store endpoint.KeyspaceGroupStorage

	// nodeBalancer is the balancer for tso nodes.
	// TODO: add user kind with different balancer when we ensure where the correspondence between tso node and user kind will be found
	nodesBalancer balancer.Balancer[string]
	// serviceRegistryMap stores the mapping from the service registry key to the service address.
	// Note: it is only used in tsoNodesWatcher.
	serviceRegistryMap map[string]string
	// tsoNodesWatcher is the watcher for the registered tso servers.
	tsoNodesWatcher *etcdutil.LoopWatcher
	// reconcileState belongs to the current API leader term. The watcher and
	// allocator for an older term retain their own state until cancellation.
	reconcileState *keyspaceGroupReconcileState
}

type keyspaceGroupReconcileEntry struct {
	id            uint32
	members       []endpoint.KeyspaceGroupMember
	transitioning bool
}

type keyspaceGroupReconcileState struct {
	sync.RWMutex
	groups map[uint32]keyspaceGroupReconcileEntry
	cancel context.CancelFunc
}

func newKeyspaceGroupReconcileState(cancel context.CancelFunc) *keyspaceGroupReconcileState {
	return &keyspaceGroupReconcileState{
		groups: make(map[uint32]keyspaceGroupReconcileEntry),
		cancel: cancel,
	}
}

func (s *keyspaceGroupReconcileState) replace(groups map[uint32]keyspaceGroupReconcileEntry) {
	s.Lock()
	s.groups = groups
	s.Unlock()
}

func (s *keyspaceGroupReconcileState) apply(updates map[uint32]*keyspaceGroupReconcileEntry) {
	s.Lock()
	defer s.Unlock()
	for groupID, entry := range updates {
		if entry == nil {
			delete(s.groups, groupID)
			continue
		}
		s.groups[groupID] = *entry
	}
}

func (s *keyspaceGroupReconcileState) groupsNeedingAllocation(
	tsoNodes map[string]string,
) []uint32 {
	s.RLock()
	defer s.RUnlock()
	var groupIDs []uint32
	for _, group := range s.groups {
		if !group.transitioning && needsNodeAllocation(group.members, tsoNodes) {
			groupIDs = append(groupIDs, group.id)
		}
	}
	slices.Sort(groupIDs)
	return groupIDs
}

func newKeyspaceGroupCache() map[endpoint.UserKind]*indexedHeap {
	groups := make(map[endpoint.UserKind]*indexedHeap, endpoint.UserKindCount)
	for userKind := range endpoint.UserKindCount {
		groups[userKind] = newIndexedHeap(int(mcs.MaxKeyspaceGroupCountInUse))
	}
	return groups
}

// NewKeyspaceGroupManager creates a Manager of keyspace group related data.
func NewKeyspaceGroupManager(
	ctx context.Context,
	store endpoint.KeyspaceGroupStorage,
	client *clientv3.Client,
) *GroupManager {
	ctx, cancel := context.WithCancel(ctx)
	m := &GroupManager{
		ctx:                ctx,
		cancel:             cancel,
		store:              store,
		groups:             newKeyspaceGroupCache(),
		client:             client,
		nodesBalancer:      balancer.GenByPolicy[string](defaultBalancerPolicy),
		serviceRegistryMap: make(map[string]string),
	}

	// If the etcd client is not nil, start the watch loop for the registered tso servers.
	// The PD(TSO) Client relies on this info to discover tso servers.
	if m.client != nil {
		m.initTSONodesWatcher(m.client)
		m.tsoNodesWatcher.StartWatchLoop()
	}
	return m
}

// Bootstrap saves default keyspace group info and init group mapping in the memory.
func (m *GroupManager) Bootstrap(ctx context.Context) error {
	// Force the membership restriction that the default keyspace must belong to default keyspace group.
	// Have no information to specify the distribution of the default keyspace group replicas, so just
	// leave the replica/member list empty. The TSO service will assign the default keyspace group replica
	// to every tso node/pod by default.
	defaultKeyspaceGroup := &endpoint.KeyspaceGroup{
		ID:        constant.DefaultKeyspaceGroupID,
		UserKind:  endpoint.Basic.String(),
		Keyspaces: []uint32{GetBootstrapKeyspaceID()},
	}

	// Ignore the error if default keyspace group already exists in the storage (e.g. PD restart/recover).
	m.Lock()
	err := m.saveKeyspaceGroups([]*endpoint.KeyspaceGroup{defaultKeyspaceGroup})
	m.Unlock()
	if err != nil && err != errs.ErrKeyspaceGroupExists {
		return err
	}

	// It will only alloc node when the group manager is on API leader.
	if m.client == nil {
		return m.reloadKeyspaceGroupCache()
	}

	termCtx, state := m.beginKeyspaceGroupReconcileTerm(ctx)
	if err := m.initKeyspaceGroupReconcileWatcher(termCtx, state); err != nil {
		m.cancelKeyspaceGroupReconcileTerm(state)
		return err
	}
	m.wg.Add(1)
	go m.allocNodesToAllKeyspaceGroups(termCtx, state)
	m.wg.Add(1)
	go m.patrolKeyspaceGroupSizeForAutoSplit(termCtx)
	return nil
}

func (m *GroupManager) reloadKeyspaceGroupCache() error {
	groups, err := m.store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	if err != nil {
		return err
	}
	cache := newKeyspaceGroupCache()
	for _, group := range groups {
		cache[endpoint.StringUserKind(group.UserKind)].Put(group)
	}
	m.Lock()
	m.groups = cache
	m.Unlock()
	return nil
}

func (m *GroupManager) beginKeyspaceGroupReconcileTerm(leaderCtx context.Context) (context.Context, *keyspaceGroupReconcileState) {
	termCtx, cancel := context.WithCancel(m.ctx)
	stopLeaderCancel := context.AfterFunc(leaderCtx, cancel)
	state := newKeyspaceGroupReconcileState(func() {
		stopLeaderCancel()
		cancel()
	})

	m.Lock()
	previousState := m.reconcileState
	m.reconcileState = state
	m.Unlock()
	if previousState != nil {
		previousState.cancel()
	}
	return termCtx, state
}

func (m *GroupManager) cancelKeyspaceGroupReconcileTerm(state *keyspaceGroupReconcileState) {
	m.Lock()
	if m.reconcileState == state {
		m.reconcileState = nil
	}
	m.Unlock()
	state.cancel()
}

// Close closes the manager.
func (m *GroupManager) Close() {
	m.cancel()
	m.Lock()
	state := m.reconcileState
	m.reconcileState = nil
	m.Unlock()
	if state != nil {
		state.cancel()
	}
	m.wg.Wait()
}

func (m *GroupManager) allocNodesToAllKeyspaceGroups(ctx context.Context, state *keyspaceGroupReconcileState) {
	defer logutil.LogPanic()
	defer m.wg.Done()
	ticker := time.NewTicker(allocNodesToKeyspaceGroupsInterval)
	failpoint.Inject("acceleratedAllocNodes", func() {
		ticker.Reset(time.Millisecond * 100)
	})
	defer ticker.Stop()
	log.Info("start to alloc nodes to all keyspace groups")
	for {
		select {
		case <-m.ctx.Done():
			// When the group manager is closed, we should stop to alloc nodes to all keyspace groups.
			// Note: If raftcluster is created failed but the group manager has been bootstrapped,
			// we need to close this goroutine by m.cancel() rather than ctx.Done() from the raftcluster.
			// because the ctx.Done() from the raftcluster will be triggered after raftcluster is created successfully.
			log.Info("server is closed, stop to alloc nodes to all keyspace groups")
			return
		case <-ctx.Done():
			// When the API leader is changed, we should stop to alloc nodes to all keyspace groups.
			log.Info("the raftcluster is closed, stop to alloc nodes to all keyspace groups")
			return
		case <-ticker.C:
			tsoNodes := uniqueTSONodes(m.nodesBalancer.GetAll())
			if len(tsoNodes) == 0 {
				continue
			}
			m.reconcileKeyspaceGroupIDs(ctx, state.groupsNeedingAllocation(tsoNodes))
		}
	}
}

func keyspaceGroupReconcileEntryFromGroup(group *endpoint.KeyspaceGroup) keyspaceGroupReconcileEntry {
	return keyspaceGroupReconcileEntry{
		id: group.ID, members: group.Members,
		transitioning: group.IsSplitting() || group.IsMerging(),
	}
}

func keyspaceGroupReconcileEntries(groups []*endpoint.KeyspaceGroup) []keyspaceGroupReconcileEntry {
	entries := make([]keyspaceGroupReconcileEntry, 0, len(groups))
	for _, group := range groups {
		entries = append(entries, keyspaceGroupReconcileEntryFromGroup(group))
	}
	return entries
}

func (m *GroupManager) reconcileKeyspaceGroups(ctx context.Context, groups []keyspaceGroupReconcileEntry) {
	tsoNodes := uniqueTSONodes(m.nodesBalancer.GetAll())
	if ctx.Err() != nil || len(tsoNodes) == 0 {
		return
	}

	groupIDs := make([]uint32, 0, len(groups))
	for _, group := range groups {
		if !group.transitioning && needsNodeAllocation(group.members, tsoNodes) {
			groupIDs = append(groupIDs, group.id)
		}
	}

	m.reconcileKeyspaceGroupIDs(ctx, groupIDs)
}

func (m *GroupManager) reconcileKeyspaceGroupIDs(ctx context.Context, groupIDs []uint32) {
	for _, groupID := range groupIDs {
		if ctx.Err() != nil {
			return
		}
		if _, err := m.allocNodesForKeyspaceGroupWithContext(ctx, groupID, nil, mcs.DefaultKeyspaceGroupReplicaCount); err != nil {
			log.Warn("failed to alloc nodes for keyspace group", zap.Uint32("keyspace-group-id", groupID), zap.Error(err))
			if shouldAbortKeyspaceGroupReconcile(err) {
				return
			}
		}
	}
}

func shouldAbortKeyspaceGroupReconcile(err error) bool {
	if goerrors.Is(err, errs.ErrEtcdKVGet) ||
		goerrors.Is(err, context.Canceled) ||
		goerrors.Is(err, context.DeadlineExceeded) ||
		goerrors.Is(err, errs.ErrNoAvailableNode) {
		return true
	}
	var etcdErr rpctypes.EtcdError
	if goerrors.As(err, &etcdErr) {
		if goerrors.Is(err, rpctypes.ErrNotLeader) {
			return true
		}
		switch etcdErr.Code() {
		case codes.Canceled, codes.DataLoss, codes.DeadlineExceeded, codes.PermissionDenied,
			codes.ResourceExhausted, codes.Unauthenticated, codes.Unavailable:
			return true
		}
		return false
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DataLoss, codes.DeadlineExceeded, codes.Internal, codes.PermissionDenied,
		codes.Unauthenticated, codes.Unavailable:
		return true
	}
	return false
}

func uniqueTSONodes(nodes []string) map[string]string {
	unique := make(map[string]string, len(nodes))
	for _, node := range nodes {
		addr := typeutil.TrimScheme(node)
		if _, ok := unique[addr]; !ok {
			unique[addr] = node
		}
	}
	return unique
}

func needsNodeAllocation(members []endpoint.KeyspaceGroupMember, tsoNodes map[string]string) bool {
	var existing [mcs.DefaultKeyspaceGroupReplicaCount]string
	count := 0
	for _, member := range members {
		addr := typeutil.TrimScheme(member.Address)
		if _, ok := tsoNodes[addr]; !ok || slices.Contains(existing[:count], addr) {
			continue
		}
		existing[count] = addr
		count++
		if count == len(existing) {
			return false
		}
	}
	return count == 0 || count != len(members) || count != len(tsoNodes)
}

// patrolKeyspaceGroupSizeForAutoSplit periodically checks all tso keyspace groups.
// If a group's keyspace count exceeds defaultKeyspaceCountSplitThreshold,
// it automatically splits a new group and moves about half of the keyspaces to the new group.
func (m *GroupManager) patrolKeyspaceGroupSizeForAutoSplit(ctx context.Context) {
	defer logutil.LogPanic()
	defer m.wg.Done()
	ticker := time.NewTicker(autoSplitKeyspaceGroupPatrolInterval)
	defer ticker.Stop()
	log.Info("start to patrol keyspace group size for auto-split")
	for {
		select {
		case <-m.ctx.Done():
			log.Info("server is closed, stop patrolling keyspace group size for auto-split")
			return
		case <-ctx.Done():
			log.Info("the raftcluster is closed, stop patrolling keyspace group size for auto-split")
			return
		case <-ticker.C:
			m.doPatrolKeyspaceGroupSizeForAutoSplit(ctx)
		}
	}
}

// doPatrolKeyspaceGroupSizeForAutoSplit checks once and performs at most one auto-split.
func (m *GroupManager) doPatrolKeyspaceGroupSizeForAutoSplit(ctx context.Context) {
	select {
	case <-m.ctx.Done():
		return
	case <-ctx.Done():
		return
	default:
	}
	groups, err := m.store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	if err != nil {
		log.Error("auto-split patrol failed to load all keyspace groups",
			zap.Error(err))
		return
	}
	defer m.reconcileKeyspaceGroups(ctx, keyspaceGroupReconcileEntries(groups))
	if len(groups) == 0 {
		return
	}
	nextID, ok := findNextAvailableKeyspaceGroupID(groups, mcs.MaxKeyspaceGroupCountInUse)
	if !ok {
		log.Warn("no available keyspace group id for auto-split, max id reached",
			zap.Uint32("max-keyspace-group-count-in-use", mcs.MaxKeyspaceGroupCountInUse))
		return
	}
	threshold := defaultKeyspaceCountSplitThreshold
	failpoint.Inject("autoSplitKeyspaceGroupThreshold", func() {
		threshold = 5
	})

	sortedGroups := make([]*endpoint.KeyspaceGroup, len(groups))
	copy(sortedGroups, groups)
	sort.Slice(sortedGroups, func(i, j int) bool {
		if len(sortedGroups[i].Keyspaces) == len(sortedGroups[j].Keyspaces) {
			return sortedGroups[i].ID < sortedGroups[j].ID
		}
		return len(sortedGroups[i].Keyspaces) > len(sortedGroups[j].Keyspaces)
	})

	for _, group := range sortedGroups {
		if group.IsSplitting() || group.IsMerging() {
			continue
		}
		if len(group.Members) < mcs.DefaultKeyspaceGroupReplicaCount {
			continue
		}
		count := len(group.Keyspaces)
		if count <= threshold {
			continue
		}
		// Split about half of the keyspaces to the new group (excluding protected bootstrap/system keyspace).
		splitIdx := count / 2
		keyspacesToMove := make([]uint32, 0, count-splitIdx)
		for i := splitIdx; i < count; i++ {
			kid := group.Keyspaces[i]
			if isProtectedKeyspaceID(kid) {
				continue
			}
			keyspacesToMove = append(keyspacesToMove, kid)
		}
		if len(keyspacesToMove) == 0 {
			log.Warn("no keyspaces to move for auto-split, skip",
				zap.Uint32("keyspace-group-id", group.ID),
				zap.Int("keyspace-count", count))
			continue
		}
		err := m.SplitKeyspaceGroupByID(group.ID, nextID, keyspacesToMove)
		if err != nil {
			log.Error("failed to auto-split keyspace group by keyspace count",
				zap.Uint32("source-id", group.ID),
				zap.Uint32("target-id", nextID),
				zap.Int("keyspace-count", count),
				zap.Int("keyspaces-to-move", len(keyspacesToMove)),
				zap.Error(err))
			return
		}
		log.Info("auto-split keyspace group by keyspace count",
			zap.Uint32("source-id", group.ID),
			zap.Uint32("target-id", nextID),
			zap.Int("keyspace-count", count),
			zap.Int("keyspaces-moved", len(keyspacesToMove)))
		return
	}
}

// findNextAvailableKeyspaceGroupID returns the smallest unused keyspace group ID in
// [1, maxCount). Group 0 is reserved for the default keyspace group.
func findNextAvailableKeyspaceGroupID(groups []*endpoint.KeyspaceGroup, maxCount uint32) (uint32, bool) {
	if maxCount <= 1 {
		return 0, false
	}
	used := make(map[uint32]struct{}, len(groups))
	for _, g := range groups {
		if g.ID < maxCount {
			used[g.ID] = struct{}{}
		}
	}
	for id := uint32(1); id < maxCount; id++ {
		if _, exists := used[id]; !exists {
			return id, true
		}
	}
	return 0, false
}

func (m *GroupManager) initTSONodesWatcher(client *clientv3.Client) {
	putFn := func(kv *mvccpb.KeyValue) error {
		s := &discovery.ServiceRegistryEntry{}
		if err := json.Unmarshal(kv.Value, s); err != nil {
			log.Warn("failed to unmarshal service registry entry",
				zap.String("event-kv-key", string(kv.Key)), zap.Error(err))
			return err
		}
		m.nodesBalancer.Put(s.ServiceAddr)
		m.serviceRegistryMap[string(kv.Key)] = s.ServiceAddr
		return nil
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		if serviceAddr, ok := m.serviceRegistryMap[key]; ok {
			delete(m.serviceRegistryMap, key)
			m.nodesBalancer.Delete(serviceAddr)
			return nil
		}
		return errors.Errorf("failed to find the service address for key %s", key)
	}

	m.tsoNodesWatcher = etcdutil.NewLoopWatcher(
		m.ctx,
		&m.wg,
		client,
		"tso-nodes-watcher",
		keypath.ServicePath(mcs.TSOServiceName),
		func([]*clientv3.Event) error { return nil },
		putFn,
		deleteFn,
		func([]*clientv3.Event) error { return nil },
		true, /* withPrefix */
	)
	m.tsoNodesWatcher.SetReconcileDeletedKeys()
}

func (m *GroupManager) initKeyspaceGroupReconcileWatcher(
	ctx context.Context,
	state *keyspaceGroupReconcileState,
) error {
	var (
		initialLoad  = true
		fullLoad     bool
		snapshot     map[uint32]keyspaceGroupReconcileEntry
		updates      map[uint32]*keyspaceGroupReconcileEntry
		initialCache map[endpoint.UserKind]*indexedHeap
		batchErr     error
	)
	compiledGroupIDPattern := keypath.GetCompiledKeyspaceGroupIDRegexp()
	recordBatchErr := func(err error) error {
		if batchErr == nil {
			batchErr = err
		}
		return err
	}
	putFn := func(kv *mvccpb.KeyValue) error {
		groupID, err := parseKeyspaceGroupID(compiledGroupIDPattern, kv.Key)
		if err != nil {
			return recordBatchErr(err)
		}

		var entry keyspaceGroupReconcileEntry
		if initialLoad {
			group := &endpoint.KeyspaceGroup{}
			if err := json.Unmarshal(kv.Value, group); err != nil {
				return recordBatchErr(errs.ErrJSONUnmarshal.Wrap(err))
			}
			entry = keyspaceGroupReconcileEntryFromGroup(group)
			initialCache[endpoint.StringUserKind(group.UserKind)].Put(group)
		} else {
			entry, err = decodeKeyspaceGroupReconcileEntry(kv.Value)
			if err != nil {
				return recordBatchErr(err)
			}
		}
		if entry.id != groupID {
			return recordBatchErr(errors.Errorf(
				"keyspace group ID %d does not match storage path ID %d", entry.id, groupID,
			))
		}
		if fullLoad {
			snapshot[groupID] = entry
		} else {
			updates[groupID] = &entry
		}
		return nil
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		groupID, err := parseKeyspaceGroupID(compiledGroupIDPattern, kv.Key)
		if err != nil {
			return recordBatchErr(err)
		}
		updates[groupID] = nil
		return nil
	}

	watcher := etcdutil.NewLoopWatcher(
		ctx,
		&m.wg,
		m.client,
		"keyspace-group-watcher",
		keypath.KeyspaceGroupIDPrefix(),
		func(events []*clientv3.Event) error {
			fullLoad = len(events) == 0
			batchErr = nil
			if fullLoad {
				snapshot = make(map[uint32]keyspaceGroupReconcileEntry)
				if initialLoad {
					initialCache = newKeyspaceGroupCache()
				}
			} else {
				updates = make(map[uint32]*keyspaceGroupReconcileEntry)
			}
			return nil
		},
		putFn,
		deleteFn,
		func([]*clientv3.Event) error {
			if batchErr != nil {
				return batchErr
			}
			if fullLoad {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				state.replace(snapshot)
				if initialLoad {
					m.Lock()
					if m.reconcileState == state {
						m.groups = initialCache
					}
					m.Unlock()
					initialLoad = false
				}
				return nil
			}
			state.apply(updates)
			return nil
		},
		true, /* withPrefix */
	)
	watcher.SetConsistentLoad()
	watcher.SetAtomicLoadCallbacks()
	watcher.SetReloadOnCompaction()
	watcher.StartWatchLoop()
	return watcher.WaitLoad()
}

func decodeKeyspaceGroupReconcileEntry(value []byte) (keyspaceGroupReconcileEntry, error) {
	decoder := json.NewDecoder(bytes.NewReader(value))
	if _, err := decoder.Token(); err != nil {
		return keyspaceGroupReconcileEntry{}, errs.ErrJSONUnmarshal.Wrap(err)
	}
	var (
		entry      keyspaceGroupReconcileEntry
		gotID      bool
		gotMembers bool
	)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return keyspaceGroupReconcileEntry{}, errs.ErrJSONUnmarshal.Wrap(err)
		}
		field, ok := token.(string)
		if !ok {
			return keyspaceGroupReconcileEntry{}, errors.New("invalid keyspace group field")
		}
		switch field {
		case "id":
			err = decoder.Decode(&entry.id)
			gotID = err == nil
		case "members":
			err = decoder.Decode(&entry.members)
			gotMembers = err == nil
		case "split-state":
			var state *endpoint.SplitState
			err = decoder.Decode(&state)
			entry.transitioning = entry.transitioning || state != nil
		case "merge-state":
			var state *endpoint.MergeState
			err = decoder.Decode(&state)
			entry.transitioning = entry.transitioning || state != nil
		default:
			var ignored json.RawMessage
			err = decoder.Decode(&ignored)
		}
		if err != nil {
			return keyspaceGroupReconcileEntry{}, errs.ErrJSONUnmarshal.Wrap(err)
		}
		// Keyspaces follows Members in KeyspaceGroup's persisted JSON. Stop here
		// so a membership watch update does not scan the large keyspace array.
		if gotID && gotMembers {
			return entry, nil
		}
	}
	return keyspaceGroupReconcileEntry{}, errors.New("keyspace group reconciliation fields are incomplete")
}

func parseKeyspaceGroupID(compiledPattern *regexp.Regexp, key []byte) (uint32, error) {
	match := compiledPattern.FindSubmatch(key)
	if match == nil {
		return 0, errors.Errorf("invalid keyspace group id path: %s", key)
	}
	id, err := strconv.ParseUint(string(match[1]), 10, 32)
	if err != nil {
		return 0, errors.Wrap(err, "failed to parse keyspace group ID")
	}
	return uint32(id), nil
}

func (m *GroupManager) putKeyspaceGroupToCacheLocked(group *endpoint.KeyspaceGroup) {
	for _, groups := range m.groups {
		groups.Remove(group.ID)
	}
	m.groups[endpoint.StringUserKind(group.UserKind)].Put(group)
}

func (m *GroupManager) removeKeyspaceGroupFromCacheLocked(groupID uint32) {
	for _, groups := range m.groups {
		groups.Remove(groupID)
	}
}

// CreateKeyspaceGroups creates keyspace groups.
func (m *GroupManager) CreateKeyspaceGroups(keyspaceGroups []*endpoint.KeyspaceGroup) error {
	m.membershipMutationLock.Lock()
	defer m.membershipMutationLock.Unlock()
	m.Lock()
	defer m.Unlock()
	if err := m.saveKeyspaceGroups(keyspaceGroups); err != nil {
		return err
	}

	for _, keyspaceGroup := range keyspaceGroups {
		m.putKeyspaceGroupToCacheLocked(keyspaceGroup)
	}

	return nil
}

// GetTSOServiceAddrs gets all TSO service addresses.
func (m *GroupManager) GetTSOServiceAddrs() []string {
	if m == nil || m.nodesBalancer == nil {
		return nil
	}
	return m.nodesBalancer.GetAll()
}

// GetKeyspaceGroups gets keyspace groups from the start ID with limit.
// If limit is 0, it will load all keyspace groups from the start ID.
func (m *GroupManager) GetKeyspaceGroups(startID uint32, limit int) ([]*endpoint.KeyspaceGroup, error) {
	return m.store.LoadKeyspaceGroups(startID, limit)
}

// GetKeyspaceGroupByID returns the keyspace group by ID.
func (m *GroupManager) GetKeyspaceGroupByID(id uint32) (*endpoint.KeyspaceGroup, error) {
	var (
		kg  *endpoint.KeyspaceGroup
		err error
	)

	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		kg, err = m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return kg, nil
}

// DeleteKeyspaceGroupByID deletes the keyspace group by ID.
func (m *GroupManager) DeleteKeyspaceGroupByID(id uint32) (*endpoint.KeyspaceGroup, error) {
	if id == constant.DefaultKeyspaceGroupID {
		return nil, errs.ErrModifyDefaultKeyspaceGroup
	}
	var (
		kg  *endpoint.KeyspaceGroup
		err error
	)

	m.membershipMutationLock.Lock()
	defer m.membershipMutationLock.Unlock()
	m.Lock()
	defer m.Unlock()
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		kg, err = m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		if kg == nil {
			return nil
		}
		if kg.IsSplitting() {
			return errs.ErrKeyspaceGroupInSplit.FastGenByArgs(id)
		}
		return m.store.DeleteKeyspaceGroup(txn, id)
	}); err != nil {
		return nil, err
	}
	if kg == nil {
		return nil, nil
	}

	// TODO: move out the keyspace to another group
	// we don't need the keyspace group as the return value
	m.removeKeyspaceGroupFromCacheLocked(id)

	return kg, nil
}

// saveKeyspaceGroups will try to save the given keyspace groups into the storage.
// If any keyspace group already exists, it will return ErrKeyspaceGroupExists.
func (m *GroupManager) saveKeyspaceGroups(keyspaceGroups []*endpoint.KeyspaceGroup) error {
	return m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		for _, keyspaceGroup := range keyspaceGroups {
			// Check if keyspace group has already existed.
			oldKG, err := m.store.LoadKeyspaceGroup(txn, keyspaceGroup.ID)
			if err != nil {
				return err
			}
			if oldKG != nil {
				return errs.ErrKeyspaceGroupExists
			}
			newKG := &endpoint.KeyspaceGroup{
				ID:        keyspaceGroup.ID,
				UserKind:  keyspaceGroup.UserKind,
				Members:   keyspaceGroup.Members,
				Keyspaces: keyspaceGroup.Keyspaces,
			}
			err = m.store.SaveKeyspaceGroup(txn, newKG)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// GetKeyspaceConfigByKind returns the keyspace config for the given user kind.
func (m *GroupManager) GetKeyspaceConfigByKind(userKind endpoint.UserKind) (map[string]string, error) {
	failpoint.Inject("assignToSpecificKeyspaceGroup", func(val failpoint.Value) {
		if groupID, ok := val.(int); ok {
			config := map[string]string{
				UserKindKey:           userKind.String(),
				TSOKeyspaceGroupIDKey: strconv.Itoa(groupID),
			}
			failpoint.Return(config, nil)
		}
	})

	if m == nil {
		return map[string]string{}, nil
	}
	m.RLock()
	defer m.RUnlock()
	return m.getKeyspaceConfigByKindLocked(userKind)
}

func (m *GroupManager) getKeyspaceConfigByKindLocked(userKind endpoint.UserKind) (map[string]string, error) {
	groups, ok := m.groups[userKind]
	if !ok {
		return map[string]string{}, errors.Errorf("user kind %s not found", userKind)
	}
	kg := groups.Top()
	if kg == nil {
		return map[string]string{}, errors.Errorf("no keyspace group for user kind %s", userKind)
	}
	id := strconv.FormatUint(uint64(kg.ID), 10)
	config := map[string]string{
		UserKindKey:           userKind.String(),
		TSOKeyspaceGroupIDKey: id,
	}
	return config, nil
}

// GetGroupByKeyspaceID returns the keyspace group ID for the given keyspace ID.
func (m *GroupManager) GetGroupByKeyspaceID(id uint32) (uint32, error) {
	m.RLock()
	defer m.RUnlock()
	for _, groups := range m.groups {
		for _, group := range groups.GetAll() {
			if slice.Contains(group.Keyspaces, id) {
				return group.ID, nil
			}
		}
	}
	return 0, errs.ErrKeyspaceNotInAnyKeyspaceGroup
}

// RemoveKeyspacesFromGroup removes the specified keyspaces from the given keyspace group.
// If a keyspace is not in the group, it will be skipped (no error). Large removals
// are split into bounded transactions. Each batch is atomic. Operations that
// relocate existing keyspaces are blocked across all batches, while newly created
// keyspaces can still be added to the group between batches.
func (m *GroupManager) RemoveKeyspacesFromGroup(
	ctx context.Context,
	groupID uint32,
	km *Manager,
	leadership *election.Leadership,
	keyspaceIDs []uint32,
) (*endpoint.KeyspaceGroup, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	term, ok := leadership.CaptureTerm()
	if !ok {
		return nil, errors.Errorf("%s because leadership term is unavailable", errs.NotLeaderErr)
	}
	return m.removeKeyspacesFromGroupWithConditions(ctx, groupID, km, keyspaceIDs, term.Comparisons())
}

func (m *GroupManager) removeKeyspacesFromGroupWithConditions(
	ctx context.Context,
	groupID uint32,
	km *Manager,
	keyspaceIDs []uint32,
	leadershipConditions []clientv3.Cmp,
) (*endpoint.KeyspaceGroup, error) {
	remainingIDs := make(map[uint32]struct{}, len(keyspaceIDs))
	for _, keyspaceID := range keyspaceIDs {
		if isProtectedKeyspaceID(keyspaceID) {
			continue
		}
		remainingIDs[keyspaceID] = struct{}{}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Keep existing keyspaces in the group stable across all batches. New
	// keyspaces may still be added between batches and are preserved because each
	// batch reloads the latest group and only removes requested IDs.
	m.membershipMutationLock.RLock()
	defer m.membershipMutationLock.RUnlock()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		kg, processedIDs, hasMore, err := m.removeKeyspacesFromGroupBatch(
			ctx, groupID, km, remainingIDs, leadershipConditions)
		if err != nil {
			return nil, err
		}
		for _, keyspaceID := range processedIDs {
			delete(remainingIDs, keyspaceID)
		}
		if !hasMore {
			return kg, nil
		}
		failpoint.InjectCall("afterRemoveKeyspacesFromGroupBatch")
	}
}

func (m *GroupManager) removeKeyspacesFromGroupBatch(
	ctx context.Context,
	groupID uint32,
	km *Manager,
	requestedIDs map[uint32]struct{},
	leadershipConditions []clientv3.Cmp,
) (*endpoint.KeyspaceGroup, []uint32, bool, error) {
	m.Lock()
	defer m.Unlock()
	return m.removeKeyspacesFromGroupBatchLocked(ctx, groupID, km, requestedIDs, leadershipConditions)
}

// removeKeyspacesFromGroupBatchLocked removes one bounded batch while the
// caller holds m's write lock.
func (m *GroupManager) removeKeyspacesFromGroupBatchLocked(
	ctx context.Context,
	groupID uint32,
	km *Manager,
	requestedIDs map[uint32]struct{},
	leadershipConditions []clientv3.Cmp,
) (*endpoint.KeyspaceGroup, []uint32, bool, error) {
	var (
		kg               *endpoint.KeyspaceGroup
		processedIDs     []uint32
		removedIDs       []uint32
		assignmentCounts = make(map[string]int)
		hasMore          bool
		err              error
	)

	runBatch := func(txn kv.Txn) error {
		// Load the keyspace group
		kg, err = m.store.LoadKeyspaceGroup(txn, groupID)
		if err != nil {
			return err
		}
		if kg == nil {
			return errs.ErrKeyspaceGroupNotExists.FastGenByArgs(groupID)
		}
		if kg.IsSplitting() {
			return errs.ErrKeyspaceGroupInSplit.FastGenByArgs(groupID)
		}
		if kg.IsMerging() {
			return errs.ErrKeyspaceGroupInMerging.FastGenByArgs(groupID)
		}

		newKeyspaces := make([]uint32, 0, len(kg.Keyspaces))
		for _, ks := range kg.Keyspaces {
			if _, requested := requestedIDs[ks]; !requested {
				newKeyspaces = append(newKeyspaces, ks)
				continue
			}
			if len(processedIDs) == maxKeyspaceRemovalBatchSize {
				hasMore = true
				newKeyspaces = append(newKeyspaces, ks)
				continue
			}

			processedIDs = append(processedIDs, ks)
			metaServiceGroupID, removed, err := km.tryRemoveKeyspaceMetadata(txn, ks)
			if err != nil {
				return err
			}
			if !removed {
				newKeyspaces = append(newKeyspaces, ks)
				continue
			}
			removedIDs = append(removedIDs, ks)
			if metaServiceGroupID != "" {
				assignmentCounts[metaServiceGroupID]++
			}
		}
		if len(removedIDs) == 0 {
			return nil
		}
		if err = km.decrementMetaServiceGroupAssignmentsTxn(txn, assignmentCounts); err != nil {
			return err
		}
		kg.Keyspaces = newKeyspaces

		return m.store.SaveKeyspaceGroup(txn, kg)
	}
	if len(leadershipConditions) > 0 {
		conditionalStore, ok := m.store.(kv.ConditionalTxnRunner)
		if !ok {
			return nil, nil, false, errors.New("keyspace group storage does not support conditional transactions")
		}
		err = conditionalStore.RunInTxnWithConditions(ctx, leadershipConditions, runBatch)
	} else {
		err = m.store.RunInTxn(ctx, runBatch)
	}
	if err != nil {
		return nil, nil, false, err
	}

	// Persistent metadata and the group membership are committed at this point,
	// so it is now safe to evict the corresponding keyspace cache entries.
	km.evictKeyspacesFromCache(removedIDs)
	// Update the cache
	m.putKeyspaceGroupToCacheLocked(kg)

	return kg, processedIDs, hasMore, nil
}

var failpointOnce sync.Once

// UpdateKeyspaceForGroup updates the keyspace field for the keyspace group.
func (m *GroupManager) UpdateKeyspaceForGroup(userKind endpoint.UserKind, groupID string, keyspaceID uint32, mutation int) error {
	if m == nil {
		return nil
	}
	id, err := strconv.ParseUint(groupID, 10, 64)
	if err != nil {
		return err
	}
	if mutation == opDelete {
		m.membershipMutationLock.Lock()
		defer m.membershipMutationLock.Unlock()
	}

	failpoint.Inject("externalAllocNode", func(val failpoint.Value) {
		failpointOnce.Do(func() {
			addrs := val.(string)
			_ = m.SetNodesForKeyspaceGroup(constant.DefaultKeyspaceGroupID, strings.Split(addrs, ","))
		})
	})
	m.Lock()
	defer m.Unlock()
	return m.updateKeyspaceForGroupLocked(userKind, id, keyspaceID, mutation)
}

func (m *GroupManager) updateKeyspaceForGroupLocked(userKind endpoint.UserKind, groupID uint64, keyspaceID uint32, mutation int) error {
	var kg *endpoint.KeyspaceGroup
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		var err error
		kg, err = m.store.LoadKeyspaceGroup(txn, uint32(groupID))
		if err != nil {
			return err
		}
		if kg == nil || endpoint.StringUserKind(kg.UserKind) != userKind {
			return errs.ErrKeyspaceGroupNotExists.FastGenByArgs(uint32(groupID))
		}
		if kg.IsSplitting() {
			return errs.ErrKeyspaceGroupInSplit.FastGenByArgs(uint32(groupID))
		}
		if kg.IsMerging() {
			return errs.ErrKeyspaceGroupInMerging.FastGenByArgs(uint32(groupID))
		}

		changed := false
		switch mutation {
		case opAdd:
			if !slice.Contains(kg.Keyspaces, keyspaceID) {
				kg.Keyspaces = append(kg.Keyspaces, keyspaceID)
				changed = true
			}
		case opDelete:
			lenOfKeyspaces := len(kg.Keyspaces)
			kg.Keyspaces = slice.Remove(kg.Keyspaces, keyspaceID)
			changed = lenOfKeyspaces != len(kg.Keyspaces)
		}
		if !changed {
			return nil
		}
		return m.store.SaveKeyspaceGroup(txn, kg)
	}); err != nil {
		return err
	}
	m.putKeyspaceGroupToCacheLocked(kg)
	return nil
}

// UpdateKeyspaceGroup updates the keyspace group.
func (m *GroupManager) UpdateKeyspaceGroup(oldGroupID, newGroupID string, oldUserKind, newUserKind endpoint.UserKind, keyspaceID uint32) error {
	if m == nil {
		return nil
	}
	m.membershipMutationLock.Lock()
	defer m.membershipMutationLock.Unlock()
	return m.updateKeyspaceGroupWithMembershipLockHeld(oldGroupID, newGroupID, oldUserKind, newUserKind, keyspaceID)
}

// updateKeyspaceGroupWithMembershipLockHeld updates the keyspace group while
// the caller holds membershipMutationLock for writing.
func (m *GroupManager) updateKeyspaceGroupWithMembershipLockHeld(
	oldGroupID, newGroupID string,
	oldUserKind, newUserKind endpoint.UserKind,
	keyspaceID uint32,
) error {
	oldID, err := strconv.ParseUint(oldGroupID, 10, 64)
	if err != nil {
		return err
	}
	newID, err := strconv.ParseUint(newGroupID, 10, 64)
	if err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()
	var (
		oldKG *endpoint.KeyspaceGroup
		newKG *endpoint.KeyspaceGroup
	)
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		var err error
		oldKG, err = m.store.LoadKeyspaceGroup(txn, uint32(oldID))
		if err != nil {
			return err
		}
		if oldKG == nil || endpoint.StringUserKind(oldKG.UserKind) != oldUserKind {
			return errors.Errorf("keyspace group %s not found in %s group", oldGroupID, oldUserKind)
		}
		if oldID == newID {
			newKG = oldKG
		} else {
			newKG, err = m.store.LoadKeyspaceGroup(txn, uint32(newID))
			if err != nil {
				return err
			}
		}
		if newKG == nil || endpoint.StringUserKind(newKG.UserKind) != newUserKind {
			return errors.Errorf("keyspace group %s not found in %s group", newGroupID, newUserKind)
		}
		if oldKG.IsSplitting() {
			return errs.ErrKeyspaceGroupInSplit.FastGenByArgs(uint32(oldID))
		} else if newKG.IsSplitting() {
			return errs.ErrKeyspaceGroupInSplit.FastGenByArgs(uint32(newID))
		} else if oldKG.IsMerging() {
			return errs.ErrKeyspaceGroupInMerging.FastGenByArgs(uint32(oldID))
		} else if newKG.IsMerging() {
			return errs.ErrKeyspaceGroupInMerging.FastGenByArgs(uint32(newID))
		}

		if !slice.Contains(newKG.Keyspaces, keyspaceID) {
			newKG.Keyspaces = append(newKG.Keyspaces, keyspaceID)
			slices.Sort(newKG.Keyspaces)
		}
		oldKG.Keyspaces = slice.Remove(oldKG.Keyspaces, keyspaceID)
		if oldID == newID {
			return m.store.SaveKeyspaceGroup(txn, oldKG)
		}
		if err = m.store.SaveKeyspaceGroup(txn, oldKG); err != nil {
			return err
		}
		return m.store.SaveKeyspaceGroup(txn, newKG)
	}); err != nil {
		return err
	}

	m.putKeyspaceGroupToCacheLocked(oldKG)
	if oldID != newID {
		m.putKeyspaceGroupToCacheLocked(newKG)
	}

	return nil
}

// SplitKeyspaceGroupByID splits the keyspace group by ID into a new keyspace group with the given new ID.
// And the keyspaces in the old keyspace group will be moved to the new keyspace group.
func (m *GroupManager) SplitKeyspaceGroupByID(
	splitSourceID, splitTargetID uint32,
	keyspaces []uint32, keyspaceIDRange ...uint32,
) error {
	var (
		splitSourceKg *endpoint.KeyspaceGroup
		splitTargetKg *endpoint.KeyspaceGroup
	)
	m.membershipMutationLock.Lock()
	defer m.membershipMutationLock.Unlock()
	m.Lock()
	defer m.Unlock()
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) (err error) {
		// Load the old keyspace group first.
		splitSourceKg, err = m.store.LoadKeyspaceGroup(txn, splitSourceID)
		if err != nil {
			return err
		}
		if splitSourceKg == nil {
			return errs.ErrKeyspaceGroupNotExists.FastGenByArgs(splitSourceID)
		}
		// A keyspace group can not take part in multiple split processes.
		if splitSourceKg.IsSplitting() {
			return errs.ErrKeyspaceGroupInSplit.FastGenByArgs(splitSourceID)
		}
		// A keyspace group can not be split when it is in merging.
		if splitSourceKg.IsMerging() {
			return errs.ErrKeyspaceGroupInMerging.FastGenByArgs(splitSourceID)
		}
		// Build the new keyspace groups for split source and target.
		var startKeyspaceID, endKeyspaceID uint32
		if len(keyspaceIDRange) >= 2 {
			startKeyspaceID, endKeyspaceID = keyspaceIDRange[0], keyspaceIDRange[1]
		}
		splitSourceKeyspaces, splitTargetKeyspaces, err := buildSplitKeyspaces(
			splitSourceKg.Keyspaces, keyspaces, startKeyspaceID, endKeyspaceID)
		if err != nil {
			return err
		}
		// Check if the source keyspace group has enough replicas.
		if len(splitSourceKg.Members) < mcs.DefaultKeyspaceGroupReplicaCount {
			return errs.ErrKeyspaceGroupNotEnoughReplicas
		}
		// Check if the new keyspace group already exists.
		splitTargetKg, err = m.store.LoadKeyspaceGroup(txn, splitTargetID)
		if err != nil {
			return err
		}
		if splitTargetKg != nil {
			return errs.ErrKeyspaceGroupExists
		}
		// Update the old keyspace group.
		splitSourceKg.Keyspaces = splitSourceKeyspaces
		splitSourceKg.SplitState = &endpoint.SplitState{
			SplitSource: splitSourceKg.ID,
		}
		splitTargetKg = &endpoint.KeyspaceGroup{
			ID: splitTargetID,
			// Keep the same user kind and members as the old keyspace group.
			UserKind:  splitSourceKg.UserKind,
			Members:   splitSourceKg.Members,
			Keyspaces: splitTargetKeyspaces,
			SplitState: &endpoint.SplitState{
				SplitSource: splitSourceKg.ID,
			},
		}
		// Save the split target keyspace group first, then save the split source keyspace group.
		// The order matters: if we save splitSourceKg first (which removes some keyspaces from it),
		// there will be a brief moment where those keyspaces don't belong to any group, causing
		// them to fallback to group 0. By saving splitTargetKg first (which contains the split
		// keyspaces), we ensure the keyspaces always belong to a valid group during the transition.

		// Create the new split keyspace group.
		if err = m.store.SaveKeyspaceGroup(txn, splitTargetKg); err != nil {
			return err
		}
		// Update the source keyspace group.
		return m.store.SaveKeyspaceGroup(txn, splitSourceKg)
	}); err != nil {
		return err
	}
	// Update the keyspace group cache.
	m.putKeyspaceGroupToCacheLocked(splitSourceKg)
	m.putKeyspaceGroupToCacheLocked(splitTargetKg)
	return nil
}

// `old` is the original keyspace list which will be split out,
// `new` is the keyspace list which will be split from the old keyspace list.
func buildSplitKeyspaces(
	old, new []uint32,
	startKeyspaceID, endKeyspaceID uint32,
) (oldSplit, newSplit []uint32, err error) {
	oldNum, newNum := len(old), len(new)
	// Split according to the new keyspace list.
	if newNum != 0 {
		if newNum > oldNum {
			return nil, nil, errs.ErrKeyspaceNotInKeyspaceGroup
		}
		var (
			oldKeyspaceMap = make(map[uint32]struct{}, oldNum)
			newKeyspaceMap = make(map[uint32]struct{}, newNum)
		)
		for _, keyspace := range old {
			oldKeyspaceMap[keyspace] = struct{}{}
		}
		for _, keyspace := range new {
			if isProtectedKeyspaceID(keyspace) {
				return nil, nil, newModifyProtectedKeyspaceError()
			}
			if _, ok := oldKeyspaceMap[keyspace]; !ok {
				return nil, nil, errs.ErrKeyspaceNotInKeyspaceGroup
			}
			newKeyspaceMap[keyspace] = struct{}{}
		}
		// Get the split keyspace list for the old keyspace group.
		oldSplit = make([]uint32, 0, oldNum-newNum)
		for _, keyspace := range old {
			if _, ok := newKeyspaceMap[keyspace]; !ok {
				oldSplit = append(oldSplit, keyspace)
			}
		}
		// If newNum != len(newKeyspaceMap), it means the provided new keyspace list contains
		// duplicate keyspaces, and we need to dedup them (https://github.com/tikv/pd/issues/6687);
		// otherwise, we can just return the old split and new keyspace list.
		if newNum == len(newKeyspaceMap) {
			return oldSplit, new, nil
		}
		newSplit = make([]uint32, 0, len(newKeyspaceMap))
		for keyspace := range newKeyspaceMap {
			newSplit = append(newSplit, keyspace)
		}
		return oldSplit, newSplit, nil
	}
	// Split according to the start and end keyspace ID.
	if startKeyspaceID == 0 && endKeyspaceID == 0 {
		return nil, nil, errs.ErrKeyspaceNotInKeyspaceGroup
	}
	newSplit = make([]uint32, 0, oldNum)
	newKeyspaceMap := make(map[uint32]struct{}, newNum)
	for _, keyspace := range old {
		if isProtectedKeyspaceID(keyspace) {
			// The source keyspace group must be the default keyspace group and we always keep
			// the bootstrap keyspace (default or system) in the default keyspace group.
			continue
		}

		if startKeyspaceID <= keyspace && keyspace <= endKeyspaceID {
			newSplit = append(newSplit, keyspace)
			newKeyspaceMap[keyspace] = struct{}{}
		}
	}
	// Check if the new keyspace list is empty.
	if len(newSplit) == 0 {
		return nil, nil, errs.ErrKeyspaceGroupWithEmptyKeyspace
	}
	// Get the split keyspace list for the old keyspace group.
	oldSplit = make([]uint32, 0, oldNum-len(newSplit))
	for _, keyspace := range old {
		if _, ok := newKeyspaceMap[keyspace]; !ok {
			oldSplit = append(oldSplit, keyspace)
		}
	}
	return oldSplit, newSplit, nil
}

// FinishSplitKeyspaceByID finishes the split keyspace group by the split target ID.
func (m *GroupManager) FinishSplitKeyspaceByID(splitTargetID uint32) error {
	var (
		splitTargetKg *endpoint.KeyspaceGroup
		splitSourceKg *endpoint.KeyspaceGroup
	)
	m.Lock()
	defer m.Unlock()

	failpoint.Inject("pauseFinishSplitBeforeTxn", nil)

	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) (err error) {
		// Load the split target keyspace group first.
		splitTargetKg, err = m.store.LoadKeyspaceGroup(txn, splitTargetID)
		if err != nil {
			return err
		}
		if splitTargetKg == nil {
			return errs.ErrKeyspaceGroupNotExists.FastGenByArgs(splitTargetID)
		}
		// Check if it's in the split state.
		if !splitTargetKg.IsSplitTarget() {
			return errs.ErrKeyspaceGroupNotInSplit.FastGenByArgs(splitTargetID)
		}
		// Load the split source keyspace group then.
		splitSourceKg, err = m.store.LoadKeyspaceGroup(txn, splitTargetKg.SplitSource())
		if err != nil {
			return err
		}
		if splitSourceKg == nil {
			return errs.ErrKeyspaceGroupNotExists.FastGenByArgs(splitTargetKg.SplitSource())
		}
		if !splitSourceKg.IsSplitSource() {
			return errs.ErrKeyspaceGroupNotInSplit.FastGenByArgs(splitTargetKg.SplitSource())
		}
		splitTargetKg.SplitState = nil
		splitSourceKg.SplitState = nil
		err = m.store.SaveKeyspaceGroup(txn, splitTargetKg)
		if err != nil {
			return err
		}
		return m.store.SaveKeyspaceGroup(txn, splitSourceKg)
	}); err != nil {
		return err
	}
	// Update the keyspace group cache.
	m.putKeyspaceGroupToCacheLocked(splitTargetKg)
	m.putKeyspaceGroupToCacheLocked(splitSourceKg)
	log.Info("finish split keyspace group", zap.Uint32("split-source-id", splitSourceKg.ID), zap.Uint32("split-target-id", splitTargetID))
	return nil
}

// GetNodesCount returns the count of nodes.
func (m *GroupManager) GetNodesCount() int {
	if m.nodesBalancer == nil {
		return 0
	}
	return len(uniqueTSONodes(m.nodesBalancer.GetAll()))
}

// AllocNodesForKeyspaceGroup allocates nodes for the keyspace group.
func (m *GroupManager) AllocNodesForKeyspaceGroup(id uint32, existMembers map[string]struct{}, desiredReplicaCount int) ([]endpoint.KeyspaceGroupMember, error) {
	return m.allocNodesForKeyspaceGroupWithContext(m.ctx, id, existMembers, desiredReplicaCount)
}

func (m *GroupManager) allocNodesForKeyspaceGroupWithContext(
	ctx context.Context,
	id uint32,
	existMembers map[string]struct{},
	desiredReplicaCount int,
) ([]endpoint.KeyspaceGroupMember, error) {
	m.Lock()
	defer m.Unlock()
	allocCtx, cancel := context.WithTimeout(ctx, allocNodesTimeout)
	defer cancel()
	ticker := time.NewTicker(allocNodesInterval)
	defer ticker.Stop()

	var (
		kg    *endpoint.KeyspaceGroup
		nodes []endpoint.KeyspaceGroupMember
	)
	err := m.store.RunInTxn(ctx, func(txn kv.Txn) error {
		var err error
		kg, err = m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		if kg == nil {
			return errs.ErrKeyspaceGroupNotExists.FastGenByArgs(id)
		}
		if kg.IsSplitting() {
			return errs.ErrKeyspaceGroupInSplit.FastGenByArgs(id)
		}
		if kg.IsMerging() {
			return errs.ErrKeyspaceGroupInMerging.FastGenByArgs(id)
		}
		tsoNodes := uniqueTSONodes(m.nodesBalancer.GetAll())
		if existMembers != nil && desiredReplicaCount > len(tsoNodes) {
			return errs.ErrNoAvailableNode
		}
		if existMembers == nil {
			existMembers = make(map[string]struct{}, len(kg.Members))
			for _, member := range kg.Members {
				if node, ok := tsoNodes[typeutil.TrimScheme(member.Address)]; ok {
					existMembers[node] = struct{}{}
				}
			}
			numExistMembers := len(existMembers)
			if (numExistMembers != 0 && numExistMembers == len(kg.Members) && numExistMembers == len(tsoNodes)) ||
				numExistMembers >= desiredReplicaCount {
				return nil
			}
		}

		nodes = make([]endpoint.KeyspaceGroupMember, 0, desiredReplicaCount)
		for addr := range existMembers {
			nodes = append(nodes, endpoint.KeyspaceGroupMember{
				Address:  addr,
				Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
			})
		}

		for len(existMembers) < desiredReplicaCount {
			select {
			case <-allocCtx.Done():
				return allocCtx.Err()
			case <-ticker.C:
			}
			if m.GetNodesCount() == 0 { // double check
				return errs.ErrNoAvailableNode
			}
			if len(existMembers) == len(tsoNodes) {
				break
			}
			addr := m.nodesBalancer.Next()
			if addr == "" {
				return errs.ErrNoAvailableNode
			}
			duplicate := false
			for existAddr := range existMembers {
				if typeutil.EqualBaseURLs(existAddr, addr) {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			existMembers[addr] = struct{}{}
			nodes = append(nodes, endpoint.KeyspaceGroupMember{
				Address:  addr,
				Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
			})
		}
		kg.Members = nodes
		return m.store.SaveKeyspaceGroup(txn, kg)
	})
	if err != nil {
		return nil, err
	}
	m.putKeyspaceGroupToCacheLocked(kg)
	if nodes == nil {
		return nil, nil
	}
	log.Info("alloc nodes for keyspace group",
		zap.Uint32("keyspace-group-id", id),
		zap.Reflect("nodes", nodes))
	return nodes, nil
}

// SetNodesForKeyspaceGroup sets the nodes for the keyspace group.
func (m *GroupManager) SetNodesForKeyspaceGroup(id uint32, nodes []string) error {
	m.Lock()
	defer m.Unlock()
	var kg *endpoint.KeyspaceGroup
	err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		var err error
		kg, err = m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		if kg == nil {
			return errs.ErrKeyspaceGroupNotExists.FastGenByArgs(id)
		}
		if kg.IsSplitting() {
			return errs.ErrKeyspaceGroupInSplit.FastGenByArgs(id)
		}
		if kg.IsMerging() {
			return errs.ErrKeyspaceGroupInMerging.FastGenByArgs(id)
		}
		members := make([]endpoint.KeyspaceGroupMember, 0, len(nodes))
		for _, node := range nodes {
			members = append(members, endpoint.KeyspaceGroupMember{
				Address:  node,
				Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
			})
		}
		kg.Members = members
		return m.store.SaveKeyspaceGroup(txn, kg)
	})
	if err != nil {
		return err
	}
	m.putKeyspaceGroupToCacheLocked(kg)
	return nil
}

// SetPriorityForKeyspaceGroup sets the priority of node for the keyspace group.
func (m *GroupManager) SetPriorityForKeyspaceGroup(id uint32, node string, priority int) error {
	m.Lock()
	defer m.Unlock()
	var kg *endpoint.KeyspaceGroup
	err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		var err error
		kg, err = m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		if kg == nil {
			return errs.ErrKeyspaceGroupNotExists.FastGenByArgs(id)
		}
		if kg.IsSplitting() {
			return errs.ErrKeyspaceGroupInSplit.FastGenByArgs(id)
		}
		if kg.IsMerging() {
			return errs.ErrKeyspaceGroupInMerging.FastGenByArgs(id)
		}
		inKeyspaceGroup := false
		members := make([]endpoint.KeyspaceGroupMember, 0, len(kg.Members))
		for _, member := range kg.Members {
			if member.IsAddressEquivalent(node) {
				inKeyspaceGroup = true
				member.Priority = priority
			}
			members = append(members, member)
		}
		if !inKeyspaceGroup {
			return errs.ErrNodeNotInKeyspaceGroup
		}
		kg.Members = members
		return m.store.SaveKeyspaceGroup(txn, kg)
	})
	if err != nil {
		return err
	}
	m.putKeyspaceGroupToCacheLocked(kg)
	log.Info("set priority for keyspace group",
		zap.Uint32("keyspace-group-id", id),
		zap.String("node", node),
		zap.Int("priority", priority))
	return nil
}

// IsExistNode checks if the node exists.
func (m *GroupManager) IsExistNode(addr string) (bool, string) {
	nodes := m.nodesBalancer.GetAll()
	for _, node := range nodes {
		if typeutil.EqualBaseURLs(node, addr) {
			return true, node
		}
	}
	return false, ""
}

// MergeKeyspaceGroups merges the keyspace group in the list into the target keyspace group.
func (m *GroupManager) MergeKeyspaceGroups(mergeTargetID uint32, mergeList []uint32) error {
	mergeListNum := len(mergeList)
	if mergeListNum == 0 {
		return nil
	}
	// The transaction below will:
	//   - Load and delete the keyspace groups in the merge list.
	//   - Load and update the target keyspace group.
	// So we pre-check the number of operations to avoid exceeding the maximum number of etcd transaction.
	if (mergeListNum+1)*2 > etcdutil.MaxEtcdTxnOps {
		return errs.ErrExceedMaxEtcdTxnOps
	}
	if slice.Contains(mergeList, constant.DefaultKeyspaceGroupID) {
		return errs.ErrModifyDefaultKeyspaceGroup
	}
	var (
		groups        = make(map[uint32]*endpoint.KeyspaceGroup, mergeListNum+1)
		mergeTargetKg *endpoint.KeyspaceGroup
	)
	m.membershipMutationLock.Lock()
	defer m.membershipMutationLock.Unlock()
	m.Lock()
	defer m.Unlock()
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) (err error) {
		// Load and check all keyspace groups first.
		for _, kgID := range append(mergeList, mergeTargetID) {
			kg, err := m.store.LoadKeyspaceGroup(txn, kgID)
			if err != nil {
				return err
			}
			if kg == nil {
				return errs.ErrKeyspaceGroupNotExists.FastGenByArgs(kgID)
			}
			// A keyspace group can not be merged if it's in splitting.
			if kg.IsSplitting() {
				return errs.ErrKeyspaceGroupInSplit.FastGenByArgs(kgID)
			}
			// A keyspace group can not be split when it is in merging.
			if kg.IsMerging() {
				return errs.ErrKeyspaceGroupInMerging.FastGenByArgs(kgID)
			}
			groups[kgID] = kg
		}
		// Build the new keyspaces for the merge target keyspace group.
		mergeTargetKg = groups[mergeTargetID]
		keyspaces := make(map[uint32]struct{})
		for _, keyspace := range mergeTargetKg.Keyspaces {
			keyspaces[keyspace] = struct{}{}
		}
		for _, kgID := range mergeList {
			kg := groups[kgID]
			for _, keyspace := range kg.Keyspaces {
				keyspaces[keyspace] = struct{}{}
			}
		}
		mergedKeyspaces := make([]uint32, 0, len(keyspaces))
		for keyspace := range keyspaces {
			mergedKeyspaces = append(mergedKeyspaces, keyspace)
		}
		sort.Slice(mergedKeyspaces, func(i, j int) bool {
			return mergedKeyspaces[i] < mergedKeyspaces[j]
		})
		mergeTargetKg.Keyspaces = mergedKeyspaces
		// Update the merge state of the target keyspace group.
		mergeTargetKg.MergeState = &endpoint.MergeState{
			MergeList: mergeList,
		}
		err = m.store.SaveKeyspaceGroup(txn, mergeTargetKg)
		if err != nil {
			return err
		}
		// Delete the keyspace groups in merge list and move the keyspaces in it to the target keyspace group.
		for _, kgID := range mergeList {
			if err := m.store.DeleteKeyspaceGroup(txn, kgID); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	// Update the keyspace group cache.
	m.putKeyspaceGroupToCacheLocked(mergeTargetKg)
	for _, kgID := range mergeList {
		m.removeKeyspaceGroupFromCacheLocked(kgID)
	}
	return nil
}

// FinishMergeKeyspaceByID finishes the merging keyspace group by the merge target ID.
func (m *GroupManager) FinishMergeKeyspaceByID(mergeTargetID uint32) error {
	var (
		mergeTargetKg *endpoint.KeyspaceGroup
		mergeList     []uint32
	)
	m.Lock()
	defer m.Unlock()
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) (err error) {
		// Load the merge target keyspace group first.
		mergeTargetKg, err = m.store.LoadKeyspaceGroup(txn, mergeTargetID)
		if err != nil {
			return err
		}
		if mergeTargetKg == nil {
			return errs.ErrKeyspaceGroupNotExists.FastGenByArgs(mergeTargetID)
		}
		// Check if it's in the merging state.
		if !mergeTargetKg.IsMergeTarget() {
			return errs.ErrKeyspaceGroupNotInMerging.FastGenByArgs(mergeTargetID)
		}
		// Make sure all merging keyspace groups are deleted.
		for _, kgID := range mergeTargetKg.MergeState.MergeList {
			kg, err := m.store.LoadKeyspaceGroup(txn, kgID)
			if err != nil {
				return err
			}
			if kg != nil {
				return errs.ErrKeyspaceGroupNotInMerging.FastGenByArgs(kgID)
			}
		}
		mergeList = mergeTargetKg.MergeState.MergeList
		mergeTargetKg.MergeState = nil
		return m.store.SaveKeyspaceGroup(txn, mergeTargetKg)
	}); err != nil {
		return err
	}
	// Update the keyspace group cache.
	m.putKeyspaceGroupToCacheLocked(mergeTargetKg)
	log.Info("finish merge keyspace group",
		zap.Uint32("merge-target-id", mergeTargetKg.ID),
		zap.Reflect("merge-list", mergeList))
	return nil
}

// MergeAllIntoDefaultKeyspaceGroup merges all other keyspace groups into the default keyspace group.
func (m *GroupManager) MergeAllIntoDefaultKeyspaceGroup() error {
	defer logutil.LogPanic()
	groupsByUserKind := m.snapshotKeyspaceGroupIDs()
	// Since we don't take the default keyspace group into account,
	// the number of unmerged keyspace groups is -1.
	unmergedGroupNum := -1
	// Calculate the total number of keyspace groups to merge.
	for _, groupIDs := range groupsByUserKind {
		unmergedGroupNum += len(groupIDs)
	}
	mergedGroupNum := 0
	// Start to merge all keyspace groups into the default one.
	for userKind, groupIDs := range groupsByUserKind {
		mergeNum := len(groupIDs)
		log.Info("start to merge all keyspace groups into the default one",
			zap.Stringer("user-kind", userKind),
			zap.Int("merge-num", mergeNum),
			zap.Int("merged-group-num", mergedGroupNum),
			zap.Int("unmerged-group-num", unmergedGroupNum))
		if mergeNum == 0 {
			continue
		}
		var (
			maxBatchSize  = etcdutil.MaxEtcdTxnOps/2 - 1
			groupsToMerge = make([]uint32, 0, maxBatchSize)
		)
		for idx, groupID := range groupIDs {
			if groupID != constant.DefaultKeyspaceGroupID {
				groupsToMerge = append(groupsToMerge, groupID)
			}
			if len(groupsToMerge) == 0 ||
				(len(groupsToMerge) < maxBatchSize && idx < mergeNum-1) {
				continue
			}
			log.Info("merge keyspace groups into the default one",
				zap.Int("index", idx),
				zap.Int("batch-size", len(groupsToMerge)),
				zap.Int("merge-num", mergeNum),
				zap.Int("merged-group-num", mergedGroupNum),
				zap.Int("unmerged-group-num", unmergedGroupNum))
			// Reach the batch size, merge them into the default keyspace group.
			if err := m.MergeKeyspaceGroups(constant.DefaultKeyspaceGroupID, groupsToMerge); err != nil {
				log.Error("failed to merge all keyspace groups into the default one",
					zap.Int("index", idx),
					zap.Int("batch-size", len(groupsToMerge)),
					zap.Int("merge-num", mergeNum),
					zap.Int("merged-group-num", mergedGroupNum),
					zap.Int("unmerged-group-num", unmergedGroupNum),
					zap.Error(err))
				return err
			}
			// Wait for the merge to finish.
			ctx, cancel := context.WithTimeout(m.ctx, time.Minute)
			ticker := time.NewTicker(time.Second)
		checkLoop:
			for {
				select {
				case <-ctx.Done():
					log.Info("cancel merging all keyspace groups into the default one",
						zap.Int("index", idx),
						zap.Int("batch-size", len(groupsToMerge)),
						zap.Int("merge-num", mergeNum),
						zap.Int("merged-group-num", mergedGroupNum),
						zap.Int("unmerged-group-num", unmergedGroupNum))
					cancel()
					ticker.Stop()
					return nil
				case <-ticker.C:
					kg, err := m.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
					if err != nil {
						log.Error("failed to check the default keyspace group merge state",
							zap.Int("index", idx),
							zap.Int("batch-size", len(groupsToMerge)),
							zap.Int("merge-num", mergeNum),
							zap.Int("merged-group-num", mergedGroupNum),
							zap.Int("unmerged-group-num", unmergedGroupNum),
							zap.Error(err))
						cancel()
						ticker.Stop()
						return err
					}
					if !kg.IsMergeTarget() {
						break checkLoop
					}
				}
			}
			cancel()
			ticker.Stop()
			mergedGroupNum += len(groupsToMerge)
			unmergedGroupNum -= len(groupsToMerge)
			groupsToMerge = groupsToMerge[:0]
		}
	}
	log.Info("finish merging all keyspace groups into the default one",
		zap.Int("merged-group-num", mergedGroupNum),
		zap.Int("unmerged-group-num", unmergedGroupNum))
	return nil
}

func (m *GroupManager) snapshotKeyspaceGroupIDs() map[endpoint.UserKind][]uint32 {
	m.RLock()
	defer m.RUnlock()
	groupsByUserKind := make(map[endpoint.UserKind][]uint32, len(m.groups))
	for userKind, groups := range m.groups {
		groupIDs := make([]uint32, 0, groups.Len())
		for _, group := range groups.GetAll() {
			groupIDs = append(groupIDs, group.ID)
		}
		groupsByUserKind[userKind] = groupIDs
	}
	return groupsByUserKind
}

// GetKeyspaceGroupPrimaryByID returns the primary node of the keyspace group by ID.
func (m *GroupManager) GetKeyspaceGroupPrimaryByID(id uint32) (string, error) {
	// check if the keyspace group exists
	kg, err := m.GetKeyspaceGroupByID(id)
	if err != nil {
		return "", err
	}
	if kg == nil {
		return "", errs.ErrKeyspaceGroupNotExists.FastGenByArgs(id)
	}

	primaryPath := keypath.ElectionPath(&keypath.MsParam{
		ServiceName: mcs.TSOServiceName,
		GroupID:     id,
	})
	leader := &tsopb.Participant{}
	ok, _, err := etcdutil.GetProtoMsgWithModRev(m.client, primaryPath, leader)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errs.ErrKeyspaceGroupPrimaryNotFound
	}
	// The format of leader name is address-groupID.
	return parsePrimaryName(leader.Name), err
}

func parsePrimaryName(name string) string {
	idx := strings.LastIndex(name, "-")
	if idx != -1 {
		return name[:idx]
	}
	return name
}
