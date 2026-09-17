// Copyright 2022 TiKV Project Authors.
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
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/id"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

const (
	// AllocStep set idAllocator's step when write persistent window boundary.
	// Use a lower value for denser idAllocation in the event of frequent pd leader change.
	AllocStep = uint64(100)
	// UserKindKey is the key for user kind in keyspace config.
	UserKindKey = "user_kind"
	// TSOKeyspaceGroupIDKey is the key for tso keyspace group id in keyspace config.
	// Note: Config[TSOKeyspaceGroupIDKey] is only used to judge whether there is keyspace group id.
	// It will not update the keyspace group id when merging or splitting.
	TSOKeyspaceGroupIDKey = "tso_keyspace_group_id"
	// GCManagementType is the key for gc_management_type in keyspace config.
	// If `gc_management_type` is `unified`, it means the current keyspace requires a tidb without 'keyspace-name'
	// configured to run a unified GC worker to calculate a unified GC state.
	// If `gc_management_type` is `keyspace_level` it means the current keyspace can calculate GC states by its own.
	GCManagementType = "gc_management_type"
	// KeyspaceLevelGC is a type of gc_management_type used to indicate that this keyspace independently manages its own
	// GC states
	KeyspaceLevelGC = "keyspace_level"
	// UnifiedGC is a type of gc_management_type used to indicate that the GC states of this keyspace is managed
	// in a unified way (managed by the NullKeyspace).
	UnifiedGC = "unified"
	// RegionBoundType is the key for region bound type in keyspace config,
	// which is used to indicate the region bound type of the keyspace.
	RegionBoundType = "region_bound_type"
	// MetaServiceGroupIDKey is the key for meta-service group id in keyspace config.
	MetaServiceGroupIDKey = "meta_service_group_id"
	// MetaServiceGroupAddressesKey is the key for meta-service group addresses in keyspace config.
	MetaServiceGroupAddressesKey = "meta_service_group_addrs"

	// WaitRegionSplitKey previously named the keyspace config entry that
	// recorded whether creation waited for the region split synchronously.
	// PD no longer writes or protects it - see CheckKeyspaceRegionBound,
	// which never read the persisted value, only ever consulted
	// tracer.waitSplit for the creation-time decision itself. Kept only so
	// CheckKeyspaceRegionBound's regression tests can name this key
	// explicitly and guard against ever special-casing it again.
	WaitRegionSplitKey = "wait_region_split"
)

// Config is the interface for keyspace config.
type Config interface {
	GetPreAlloc() []string
	ToWaitRegionSplit() bool
	GetWaitRegionSplitTimeout() time.Duration
	GetCheckRegionSplitInterval() time.Duration
	// GetMetaServiceGroups returns the meta-service groups for keyspace assignment.
	// key is the meta-service group id and value is the meta-service group addresses.
	SetMetaServiceGroups(map[string]string)
	GetMetaServiceGroups() map[string]string
}

// Manager manages keyspace related data.
// It validates requests and provides concurrency control.
type Manager struct {
	// ctx is the context of the manager, to be used in transaction.
	ctx context.Context
	// metaLock guards keyspace meta.
	metaLock *syncutil.LockGroup
	// idAllocator allocates keyspace id.
	idAllocator id.Allocator
	// store is the storage for keyspace related information.
	store endpoint.KeyspaceStorage
	// rc is the raft cluster of the server.
	cluster core.ClusterInformer
	// config is the configurations of the manager.
	config Config
	// kgm is the keyspace group manager of the server.
	kgm *GroupManager
	// mgm is the meta-service group manager of the server.
	mgm *MetaServiceGroupManager
	// nextPatrolStartID is the next start id of keyspace assignment patrol.
	nextPatrolStartID uint32
	// cached keyspace meta info for each keyspace ID.
	keyspaceNameLookup  sync.Map // store as ID(uint32) -> name(string)
	keyspaceStateLookup sync.Map // store as ID(uint32) -> state(keyspacepb.KeyspaceState)
	// txnLock is used to serialize create keyspace in different keyspace groups, avoid to etcd put conflicts.
	txnLock *syncutil.LockGroup
}

// CreateKeyspaceRequest represents necessary arguments to create a keyspace.
type CreateKeyspaceRequest struct {
	// Name of the keyspace to be created.
	// Using an existing name will result in error.
	Name   string
	Config map[string]string
	// CreateTime is the timestamp used to record creation time.
	CreateTime int64
}

// CreateKeyspaceByIDRequest represents necessary arguments to create a keyspace.
type CreateKeyspaceByIDRequest struct {
	// ID of the keyspace to be created.
	// Using an existing ID will result in error.
	ID *uint32
	// Name of the keyspace to be created.
	// Using an existing name will result in error.
	Name   string
	Config map[string]string
	// CreateTime is the timestamp used to record creation time.
	CreateTime int64
}

// NewKeyspaceManager creates a Manager of keyspace related data.
func NewKeyspaceManager(
	ctx context.Context,
	store endpoint.KeyspaceStorage,
	cluster core.ClusterInformer,
	idAllocator id.Allocator,
	config Config,
	kgm *GroupManager,
	mgm *MetaServiceGroupManager,
) *Manager {
	manager := &Manager{
		ctx: ctx,
		// Remove the lock of the given key from the lock group when unlock to
		// keep minimal working set, which is suited for low qps, non-time-critical
		// and non-consecutive large key space scenarios. One of scenarios for
		// last use case is keyspace group split loads non-consecutive keyspace meta
		// in batches and lock all loaded keyspace meta within a batch at the same time.
		metaLock:          syncutil.NewLockGroup(syncutil.WithRemoveEntryOnUnlock(true)),
		idAllocator:       idAllocator,
		store:             store,
		cluster:           cluster,
		config:            config,
		kgm:               kgm,
		mgm:               mgm,
		nextPatrolStartID: constant.StartKeyspaceID,
		txnLock:           syncutil.NewLockGroup(syncutil.WithRemoveEntryOnUnlock(true)),
	}
	// Let the meta-service group manager validate group deletion against actual
	// keyspace assignments instead of the drift-prone persisted counter.
	if mgm != nil {
		mgm.SetKeyspaceAssignmentCounter(manager.CountKeyspacesByMetaServiceGroup)
	}
	return manager
}

// Bootstrap saves default keyspace info.
func (manager *Manager) Bootstrap() error {
	bootstrapKeyspaceID := GetBootstrapKeyspaceID()
	bootstrapKeyspaceName := GetBootstrapKeyspaceName()
	err := manager.initReserveKeyspace(bootstrapKeyspaceID, bootstrapKeyspaceName)
	if err != nil {
		return err
	}
	// Initialize pre-alloc keyspace.
	preAlloc := manager.config.GetPreAlloc()
	for _, keyspaceName := range preAlloc {
		go func() {
			for range 3 {
				config, err := manager.kgm.GetKeyspaceConfigByKind(endpoint.Basic)
				if err != nil {
					log.Error("[keyspace] failed to get keyspace config for pre-alloc keyspace", zap.String("keyspaceName", keyspaceName), zap.Error(err))
					continue
				}
				req := &CreateKeyspaceRequest{
					Name:       keyspaceName,
					CreateTime: time.Now().Unix(),
					Config:     config,
				}
				_, err = manager.CreateKeyspace(req)
				// Ignore the keyspaceExists error for the same reason as saving default keyspace.
				if err != nil && err != errs.ErrKeyspaceExists {
					log.Error("[keyspace] failed to create pre-alloc keyspace", zap.String("keyspaceName", keyspaceName), zap.Error(err))
					time.Sleep(time.Second)
					continue
				}
				return
			}
		}()
	}
	return nil
}

func (manager *Manager) initReserveKeyspace(id uint32, name string) error {
	tracer := &createKeyspaceTracer{userKind: endpoint.Basic}
	tracer.waitSplit = false
	tracer.Begin()
	tracer.SetKeyspace(id, name)
	tracer.OnAllocateIDFinished()
	config, err := manager.kgm.GetKeyspaceConfigByKind(endpoint.Basic)
	if err != nil {
		return err
	}
	// It is needed to set for system keyspace in next-gen.
	if id == constant.SystemKeyspaceID {
		config[GCManagementType] = KeyspaceLevelGC
	}
	tracer.OnGetConfigFinished()
	now := time.Now().Unix()
	_, err = manager.createKeyspaceWithoutCheck(tracer, config, now)
	if err == nil {
		return nil
	}
	if err != errs.ErrKeyspaceExists {
		return err
	}
	// The reserved keyspace meta already exists (e.g. PD restart): the atomic
	// creation transaction aborted before reaching the group-membership and
	// region-label-rule steps, so repair them explicitly here.
	return manager.repairReservedKeyspace(id)
}

// repairReservedKeyspace ensures the reserved keyspace's TSO keyspace-group
// membership and region label rule exist, even when its meta was already
// created by an earlier Bootstrap call. It repairs against the keyspace's own
// persisted config (not a freshly computed default), so it cannot move the
// keyspace to a different group than the one it is actually recorded as
// belonging to. Both underlying operations are idempotent (SaveRegionRule
// overwrites unconditionally; the group op is a no-op if the keyspace is
// already a member), so this is safe to run on every restart. The post-commit
// callbacks are invoked once RunTxn succeeds, so the in-memory TSO group cache
// and region labeler rule index are refreshed along with storage, not just
// storage on its own.
func (manager *Manager) repairReservedKeyspace(id uint32) error {
	meta, err := manager.LoadKeyspaceByID(id)
	if err != nil {
		return err
	}
	groupIDStr := meta.GetConfig()[TSOKeyspaceGroupIDKey]
	boundType := keyTypeStringToRegionBoundType(meta.GetConfig()[RegionBoundType])

	txnOps := make([]txnOp, 0, 2)
	txnCbs := make([]txnCb, 0, 2)
	addTxn := func(op txnOp, cb txnCb) {
		if op != nil {
			txnOps = append(txnOps, op)
		}
		if cb != nil {
			txnCbs = append(txnCbs, cb)
		}
	}

	var groupID uint64
	if groupIDStr != "" {
		op, cb, err := manager.kgm.updateKeyspaceForGroupTxnOp(endpoint.Basic, groupIDStr, id, opAdd)
		if err != nil {
			return err
		}
		addTxn(op, cb)
		groupID, err = strconv.ParseUint(groupIDStr, 10, 32)
		if err != nil {
			return err
		}
	}

	op, cb, err := manager.saveKeyspaceRegionLabelerTxnOp(id, boundType)
	if err != nil {
		return err
	}
	addTxn(op, cb)

	if err := manager.RunTxn(uint32(groupID), txnOps); err != nil {
		return err
	}
	for _, cb := range txnCbs {
		cb(nil)
	}
	return nil
}

// UpdateConfig update keyspace manager's config.
func (manager *Manager) UpdateConfig(cfg Config) {
	manager.config = cfg
	if manager.mgm != nil {
		manager.mgm.updateGroups(cfg.GetMetaServiceGroups())
	}
}

// CreateKeyspace create a keyspace meta with given config and save it to storage.
// todo: make all etcd operators in one txn to make the operation atomic.
func (manager *Manager) CreateKeyspace(request *CreateKeyspaceRequest) (*keyspacepb.KeyspaceMeta, error) {
	return manager.createKeyspaceInner(request.Name, request.Config, request.CreateTime)
}

func (manager *Manager) createKeyspaceInner(name string, config map[string]string, createTime int64, ids ...uint32) (*keyspacepb.KeyspaceMeta, error) {
	tracer := &createKeyspaceTracer{}
	tracer.Begin()
	// Validate purposed name's legality.
	if err := validateName(name); err != nil {
		return nil, err
	}
	// Check if keyspace with that name already exists before allocating ID.
	// This prevents unnecessary ID allocation when the name already exists.
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		nameExists, _, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil {
			return err
		}
		if nameExists {
			return errs.ErrKeyspaceExists
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var newID uint32
	if len(ids) > 0 {
		newID = ids[0]
		if err = validateID(newID); err != nil {
			return nil, err
		}
	} else {
		// Allocate new keyspaceID.
		newID, err = manager.allocID()
		if err != nil {
			return nil, err
		}
	}
	tracer.SetKeyspace(newID, name)
	tracer.waitSplit = manager.config.ToWaitRegionSplit()
	tracer.OnAllocateIDFinished()
	if isProtectedKeyspaceID(newID) {
		err := newModifyProtectedKeyspaceError()
		log.Warn("[keyspace] failed to update keyspace config", errs.ZapError(err))
		return nil, err
	}
	// The TSO keyspace group and meta-service group are assigned by PD, so drop
	// any client-provided values to avoid them leaking into the saved config.
	if config != nil {
		delete(config, MetaServiceGroupAddressesKey)
		delete(config, MetaServiceGroupIDKey)
		delete(config, TSOKeyspaceGroupIDKey)
	}
	// Get keyspace config.
	userKind := endpoint.StringUserKind(config[UserKindKey])
	tracer.userKind = userKind
	ksConfig, err := manager.kgm.GetKeyspaceConfigByKind(userKind)
	if err != nil {
		return nil, err
	}
	if len(ksConfig) != 0 {
		if config == nil {
			config = ksConfig
		} else {
			config[TSOKeyspaceGroupIDKey] = ksConfig[TSOKeyspaceGroupIDKey]
			config[UserKindKey] = ksConfig[UserKindKey]
		}
	}

	// Set default value of GCManagementType to KeyspaceLevelGC for NextGen
	if kerneltype.IsNextGen() {
		if config == nil {
			config = make(map[string]string)
		}
		if v, ok := config[GCManagementType]; !ok || len(v) == 0 {
			config[GCManagementType] = KeyspaceLevelGC
		}
	}
	tracer.OnGetConfigFinished()
	return manager.createKeyspaceWithoutCheck(tracer, config, createTime)
}

func (manager *Manager) createKeyspaceWithoutCheck(tracer *createKeyspaceTracer, config map[string]string, createTime int64) (*keyspacepb.KeyspaceMeta, error) {
	boundType := manager.getRegionBoundType()
	if config == nil {
		config = make(map[string]string)
	}
	config[RegionBoundType] = boundType.String()
	// Create a disabled keyspace meta for tikv-server to get the config on keyspace split.
	keyspace := &keyspacepb.KeyspaceMeta{
		Keyspace:       &keyspacepb.KeyspaceMeta_Id{Id: tracer.keyspaceID},
		Name:           tracer.keyspaceName,
		State:          keyspacepb.KeyspaceState_DISABLED,
		CreatedAt:      createTime,
		StateChangedAt: createTime,
		Config:         config,
	}
	// if waitSplit is false, we can enable the keyspace directly, otherwise we need to wait for the split to finish.
	if !tracer.waitSplit {
		keyspace.State = keyspacepb.KeyspaceState_ENABLED
	}

	txnOps := make([]txnOp, 0, 4)
	txnCbs := make([]txnCb, 0, 4)
	addTxn := func(op txnOp, cb txnCb) {
		if op != nil {
			txnOps = append(txnOps, op)
		}
		if cb != nil {
			txnCbs = append(txnCbs, cb)
		}
	}
	// Assign a meta-service group (if any exist and the keyspace is not protected)
	// within the same transaction that persists the keyspace meta, so the
	// assignment count and the keyspace are committed atomically. The op runs
	// before saveNewKeyspaceTxnOp so the assigned group id becomes part of the
	// saved meta config. The mgm read lock is held across the whole txn (see
	// runCreateKeyspaceTxn) so a concurrent group deletion cannot leave the
	// keyspace referencing a removed group.
	assignToMetaServiceGroup := manager.mgm.hasGroups() &&
		!isProtectedKeyspaceID(tracer.keyspaceID)
	if assignToMetaServiceGroup {
		op, cb := manager.assignMetaServiceGroupTxnOp(keyspace)
		addTxn(op, cb)
	}
	op, cb := manager.saveNewKeyspaceTxnOp(keyspace)
	addTxn(op, cb)
	op, cb, err := manager.kgm.updateKeyspaceForGroupTxnOp(tracer.userKind, config[TSOKeyspaceGroupIDKey], tracer.keyspaceID, opAdd)
	if err != nil {
		log.Warn("[keyspace] failed to update keyspace group ", errs.ZapError(err))
		return nil, err
	}
	addTxn(op, cb)

	op, cb, err = manager.saveKeyspaceRegionLabelerTxnOp(tracer.keyspaceID, boundType)
	if err != nil {
		log.Warn("[keyspace] failed to prepare split keyspace region operation", errs.ZapError(err))
		return nil, err
	}
	addTxn(op, cb)
	// In classic mode the keyspace group manager is nil, so the config carries no
	// TSO keyspace group id. Fall back to the default keyspace group id (0), which
	// here is only used to serialize the creation transaction.
	var groupID uint64
	if gid := config[TSOKeyspaceGroupIDKey]; gid != "" {
		groupID, err = strconv.ParseUint(gid, 10, 64)
		if err != nil {
			return nil, err
		}
	}
	err = manager.runCreateKeyspaceTxn(assignToMetaServiceGroup, uint32(groupID), txnOps)
	for _, cb := range txnCbs {
		if cb != nil {
			cb(err)
		}
	}
	if err != nil {
		log.Warn("[keyspace] txn execute failed", errs.ZapError(err))
		return nil, err
	}

	tracer.OnSaveKeyspaceMetaFinished()

	// Split keyspace region.
	if tracer.waitSplit {
		err = manager.waitKeyspaceRegionSplit(tracer.keyspaceID, boundType)
		failpoint.Inject("waitSplitKeyspaceFailed", func() {
			err = errors.New("failpoint triggered: waitSplitKeyspaceFailed")
		})
		if err == nil {
			// only enable the keyspace after the split is finished, so that the keyspace is not used before the split is finished.
			err = manager.enableNewKeyspace(tracer.keyspaceID, createTime)
			if err == nil {
				keyspace.State = keyspacepb.KeyspaceState_ENABLED
				keyspace.StateChangedAt = createTime
			}
		}
		if err != nil {
			if rbErr := manager.rollbackCreateKeyspace(keyspace); rbErr != nil {
				log.Warn("[create-keyspace] failed to rollback keyspace after enable failed",
					zap.Uint32("keyspace-id", tracer.keyspaceID),
					zap.String("keyspace-name", tracer.keyspaceName),
					errs.ZapError(rbErr),
				)
			}
			return nil, err
		}
	}

	tracer.OnSplitRegionFinished()
	tracer.OnCreateKeyspaceComplete()

	log.Info("[create-keyspace] keyspace created",
		zap.Uint32("keyspace-id", keyspace.GetId()),
		zap.String("keyspace-name", keyspace.GetName()),
	)
	return keyspace, nil
}

// rollbackCreateKeyspace undoes what createKeyspaceWithoutCheck committed.
// The keyspace object passed in reflects a snapshot from when the create
// transaction committed; by the time wait-split fails, an independent
// operation (a state change, a config PATCH, or a TSO keyspace-group
// split/merge) can have moved things on:
//
//   - The state API can transition this keyspace out of DISABLED (e.g. to
//     ENABLED) while wait-split is still running elsewhere - nothing tracks
//     "a create is in flight" as distinct from "is currently DISABLED". If
//     that happened, this keyspace's lifecycle is no longer this failed
//     create's to manage, and deleting it would destroy something another
//     operation has legitimately taken over.
//   - A TSO keyspace-group split/merge moves a keyspace between groups by
//     rewriting the groups' own Keyspaces lists; it never touches the
//     keyspace's own Config[TSOKeyspaceGroupIDKey]. So even a keyspace still
//     correctly DISABLED can have that field pointing at a group it no
//     longer belongs to.
//   - Once the meta is deleted, this keyspace's id is free to be reused by
//     an unrelated CreateKeyspaceByID call. If TSO group cleanup ran after
//     that, resolving membership by id could hit the new keyspace's group
//     instead and strip it.
//
// To stay correct under all of these, rollback re-derives the truth from
// durable state right before acting, and - rather than trying to keep
// re-checking a moving target through the rest of the function - permanently
// seals the keyspace against further mutation as early as possible:
//
//  1. Reload the keyspace under metaLock. If it's gone, there's nothing to
//     roll back. If it's no longer DISABLED, skip rollback entirely rather
//     than deleting a keyspace something else has taken over. This is a
//     cheap early exit; step 2 below re-verifies the same thing atomically
//     with its own write, so this step's result is not load-bearing.
//  2. Seal the keyspace by transitioning it straight to TOMBSTONE, under
//     metaLock, with an identity check (id, still DISABLED, expected name)
//     folded into the same transaction as the write - see
//     sealKeyspaceForRollback. TOMBSTONE is a terminal state:
//     stateTransitionTable only allows TOMBSTONE -> TOMBSTONE, and
//     allowChangeConfig does not include it either, so once this commits,
//     no concurrent UpdateKeyspaceState(ByID) can ever move it back to
//     ENABLED and no concurrent config PATCH can touch it - permanently and
//     unconditionally, not just for a narrow window. That closes the state-
//     takeover and PATCH-driven group-reassignment races at the root,
//     rather than needing metaLock held across the rest of this function to
//     keep re-checking for them.
//  3. Best-effort undo of the TSO keyspace-group membership. The meta still
//     exists (step 2 changed its state, not removed it), so the id can't be
//     reused out from under this step. TOMBSTONE blocks state/config-driven
//     group changes, but not a TSO keyspace-group split/merge, which moves
//     keyspaces between groups without consulting the keyspace's own state
//     at all - so this first tries the group named by the sealed keyspace's
//     own Config[TSOKeyspaceGroupIDKey] (current as of the reload inside
//     sealKeyspaceForRollback), storage-verified within the delete's own
//     transaction, before falling back to GetGroupByKeyspaceID - the
//     GroupManager's cache of current membership, which can briefly lag
//     storage behind a concurrent creation or rollback's own group-op
//     commit - re-read on every retry attempt and re-verified after a
//     successful-looking delete (see undoTSOKeyspaceGroupMembership). The
//     group may be mid split/merge and reject this for a while (bounded, so
//     a short retry usually clears it); if it still fails after retrying,
//     step 4 below still runs unconditionally, so this only leaves a stale
//     membership entry in the group - logged for follow-up - instead of
//     blocking the rollback.
//  4. Remove the meta, undo the meta-service group assignment count, and
//     remove the region label rule, all in one transaction, retried a few
//     times since a transient failure here must not strand the keyspace
//     name permanently. Unconditional on step 3's outcome: the alternative
//     failure mode - meta present but with no TSO group membership -
//     reproduces exactly the inconsistent state this PR exists to eliminate
//     (see #10461), and would also leave the keyspace name stuck. A stale
//     group membership entry with no backing meta (step 3 exhausting its
//     retries) is comparatively harmless.
//
// metaLock is held for steps 1, 2, and 4, but released for step 3:
// updateKeyspaceForGroupTxnOp (and the callback it returns) briefly takes
// GroupManager.Lock, while RemoveKeyspacesFromGroup takes GroupManager.Lock
// first and then metaLock (via Manager.RemoveKeyspace) — holding metaLock
// across step 3 would invert that order and can deadlock against a
// concurrent RemoveKeyspacesFromGroup. Steps 1, 2, and 4 never touch
// GroupManager.Lock, so holding metaLock for each of them (independently,
// not across step 3) carries no such risk.
func (manager *Manager) rollbackCreateKeyspace(keyspace *keyspacepb.KeyspaceMeta) error {
	var regionLabeler *labeler.RegionLabeler
	var ruleID string
	if cl, ok := manager.cluster.(interface{ GetRegionLabeler() *labeler.RegionLabeler }); ok {
		regionLabeler = cl.GetRegionLabeler()
		ruleID = getRegionLabelID(keyspace.GetId())
	}
	id := keyspace.GetId()

	// Step 1: cheap early exit against durable storage instead of trusting
	// the snapshot - see the function comment for why. expectedName is
	// fixed here, before any reload, and never re-derived from a later
	// snapshot: see reloadKeyspaceIfStillDisabled for why that matters.
	expectedName := keyspace.GetName()
	verified, err := manager.reloadKeyspaceIfStillDisabled(id, expectedName)
	if err != nil {
		return err
	}
	if verified == nil {
		return nil
	}

	// Step 2: seal the keyspace as TOMBSTONE - see the function comment for
	// why this closes the remaining races permanently instead of narrowing
	// them.
	sealed, err := manager.sealKeyspaceForRollback(id, expectedName)
	if err != nil {
		return err
	}
	if sealed == nil {
		return nil
	}
	// sealKeyspaceForRollback cannot do this itself - see its function
	// comment - since it's still holding metaLock when it returns.
	manager.refreshKeyspaceMetaCache(id)
	keyspace = sealed

	// Step 3: best-effort, tolerating a group mid split/merge - see the
	// function comment. knownGroupID/knownUserKind come from the sealed
	// keyspace's own config, current as of sealKeyspaceForRollback's reload.
	knownGroupID := keyspace.GetConfig()[TSOKeyspaceGroupIDKey]
	knownUserKind := endpoint.StringUserKind(keyspace.GetConfig()[UserKindKey])
	if err := manager.undoTSOKeyspaceGroupMembership(id, expectedName, knownGroupID, knownUserKind); err != nil {
		log.Error("[keyspace] rollback could not undo TSO keyspace-group membership; "+
			"the group may still list a keyspace whose metadata is about to be removed",
			zap.Uint32("keyspace-id", id),
			errs.ZapError(err),
		)
	}

	// Step 4: remove the meta and the region label rule, in one transaction,
	// retried a few times since the create transaction has already
	// committed and a transient failure here must not strand the keyspace
	// name permanently. The meta-service group assignment count was already
	// unassigned as part of sealing the keyspace in step 2, so this step
	// only needs to touch the meta and the label rule.
	manager.metaLock.Lock(id)
	metaTxnOps := []txnOp{func(txn kv.Txn) error {
		if err := manager.store.RemoveKeyspace(txn, keyspace.GetId(), keyspace.GetName()); err != nil {
			return err
		}
		if regionLabeler == nil {
			return nil
		}
		return regionLabeler.GetRuleStorage().DeleteRegionRule(txn, ruleID)
	}}
	for i := range 3 {
		err = manager.RunTxn(0, metaTxnOps)
		if err == nil {
			break
		}
		if i < 2 {
			time.Sleep(time.Second)
		}
	}
	manager.metaLock.Unlock(id)
	if err != nil {
		return err
	}

	manager.refreshKeyspaceMetaCache(id)
	if regionLabeler != nil {
		// Re-read from storage rather than unconditionally deleting the
		// cache entry: nothing reserves the "keyspaces/<id>" rule ID
		// against the generic label-rule API, so a concurrent SetLabelRule
		// on the same id could have already persisted and cached a
		// replacement between our delete committing and this call - a bare
		// delete would then remove that newer cache entry, leaving storage
		// with the replacement but the cache believing it doesn't exist.
		// See saveKeyspaceRegionLabelerTxnOp's callback for the same
		// pattern.
		found, err := regionLabeler.ReloadRuleFromStorage(ruleID)
		if err != nil {
			log.Error("failed to reload region label rule into cache after rollback, cache may be stale",
				zap.Uint32("keyspace-id", id), zap.String("rule-id", ruleID), errs.ZapError(err))
		} else if found {
			log.Warn("region label rule was recreated by a concurrent writer before rollback's delete could be cached",
				zap.Uint32("keyspace-id", id), zap.String("rule-id", ruleID))
		}
	}
	return nil
}

// reloadKeyspaceIfStillDisabled reloads the keyspace under metaLock and
// returns it only if it still exists, is still DISABLED, and still has
// expectedName. The name check matters because id alone does not identify
// the keyspace this rollback is for: if the original keyspace was archived
// and removed while its create call was still waiting on region split,
// CreateKeyspaceByID could have reused id for an unrelated keyspace by the
// time rollback runs - checking only id and state would then adopt that
// replacement's identity and let rollback delete it instead of silently
// doing nothing. A nil meta and nil error together mean the caller should
// stop: either the keyspace is gone, its identity no longer matches, or
// something else has taken over its lifecycle since it was created (logged
// here either way).
func (manager *Manager) reloadKeyspaceIfStillDisabled(id uint32, expectedName string) (*keyspacepb.KeyspaceMeta, error) {
	manager.metaLock.Lock(id)
	defer manager.metaLock.Unlock(id)
	var current *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		var err error
		current, err = manager.store.LoadKeyspaceMeta(txn, id)
		return err
	})
	if err != nil {
		return nil, err
	}
	if current == nil {
		return nil, nil
	}
	if current.GetName() != expectedName {
		log.Warn("[keyspace] skipping create rollback: id now identifies a different keyspace",
			zap.Uint32("keyspace-id", id),
			zap.String("expected-name", expectedName),
			zap.String("current-name", current.GetName()),
		)
		return nil, nil
	}
	if current.GetState() != keyspacepb.KeyspaceState_DISABLED {
		log.Warn("[keyspace] skipping create rollback: keyspace was taken over externally during creation",
			zap.Uint32("keyspace-id", id),
			zap.String("state", current.GetState().String()),
		)
		return nil, nil
	}
	return current, nil
}

// sealKeyspaceForRollback marks the keyspace TOMBSTONE, closing the state-
// takeover and PATCH-driven group-reassignment races permanently rather
// than narrowing them - see the function comment on rollbackCreateKeyspace.
// The identity check (id, still DISABLED, expectedName) and the write are
// folded into the same transaction, the same reasoning as
// undoTSOKeyspaceGroupMembership's identityCheckOp: the two either commit
// together or not at all.
//
// This writes the target state directly rather than going through
// transformKeyspaceState: that function's stateTransitionTable does not
// allow DISABLED -> TOMBSTONE (only ARCHIVED -> TOMBSTONE, the normal
// user-driven lifecycle) - but this is not a normal lifecycle transition,
// it's rollback's own internal-only way of permanently disabling further
// mutation of a keyspace whose creation failed.
// unassignKeyspaceFromMetaServiceGroup is still called directly here,
// matching what transformKeyspaceState itself does for a TOMBSTONE
// transition, so the assignment count doesn't need to wait for step 4 to
// unwind it.
//
// Returns nil, nil (not an error) if the keyspace is gone or no longer
// matches (id, DISABLED, expectedName): there is nothing to roll back,
// logged here either way. A non-nil error means the transaction itself
// could not be committed after retrying - the keyspace is left DISABLED,
// and the caller should propagate the error rather than proceed. On success
// the caller is responsible for refreshing the cache (see
// refreshKeyspaceMetaCache) once this call returns - this function cannot
// do that itself, since that helper takes metaLock and this function is
// still holding it (via defer) until it returns.
func (manager *Manager) sealKeyspaceForRollback(id uint32, expectedName string) (*keyspacepb.KeyspaceMeta, error) {
	manager.metaLock.Lock(id)
	defer manager.metaLock.Unlock(id)
	var sealed *keyspacepb.KeyspaceMeta
	sealOp := func(txn kv.Txn) error {
		current, err := manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if current == nil || current.GetState() != keyspacepb.KeyspaceState_DISABLED || current.GetName() != expectedName {
			// Not an error: retrying cannot help an identity mismatch, so
			// this deliberately lets the loop below stop immediately
			// (RunTxn still returns nil) with sealed left unset.
			return nil
		}
		current.State = keyspacepb.KeyspaceState_TOMBSTONE
		current.StateChangedAt = time.Now().Unix()
		if err := manager.unassignKeyspaceFromMetaServiceGroup(txn, current); err != nil {
			return err
		}
		if err := manager.store.SaveKeyspaceMeta(txn, current); err != nil {
			return err
		}
		sealed = current
		return nil
	}
	var err error
	for i := range 3 {
		err = manager.RunTxn(0, []txnOp{sealOp})
		if err == nil {
			break
		}
		if i < 2 {
			time.Sleep(time.Second)
		}
	}
	if err != nil {
		return nil, err
	}
	if sealed == nil {
		log.Warn("[keyspace] skipping create rollback: keyspace is gone, was taken over externally during creation, or now identifies a different keyspace",
			zap.Uint32("keyspace-id", id),
			zap.String("expected-name", expectedName),
		)
		return nil, nil
	}
	// The caller refreshes the cache once metaLock above is released:
	// refreshKeyspaceMetaCache takes metaLock itself, and syncutil.LockGroup
	// is not reentrant, so calling it here while still holding the lock
	// (deferred above) would self-deadlock.
	return sealed, nil
}

// errRollbackTargetChanged means the keyspace this rollback is cleaning up
// (identified by id and expectedName together, since an id alone can be
// reused by an unrelated keyspace) is no longer the TOMBSTONE keyspace
// sealKeyspaceForRollback sealed under that same identity, by the time the
// TSO group membership delete was about to commit. Not a transient failure:
// retrying it cannot succeed, since something else has changed this
// keyspace's identity (or reused its id) - TOMBSTONE itself cannot be
// changed away from by any normal path, so in practice this only fires if
// the id has already been reused, which step 4 of rollbackCreateKeyspace
// running after this guards against by construction (the meta this checks
// against still exists until step 4 deletes it).
var errRollbackTargetChanged = errors.New("keyspace no longer identifies the same sealed keyspace, not rolling back its TSO group membership")

// errRollbackGroupMembershipMoved means a delete attempt committed without
// error but keyspaceID was still found in a group afterward: the delete had
// already become a no-op by commit time because keyspaceID had moved to a
// different group (e.g. via a TSO keyspace-group split/merge, the only kind
// of group change a sealed TOMBSTONE keyspace remains subject to - see the
// function comment on rollbackCreateKeyspace). Not the same as
// errRollbackTargetChanged: the keyspace's own identity is still intact,
// only its group membership moved, so retrying against the freshly-resolved
// group can still succeed.
var errRollbackGroupMembershipMoved = errors.New("keyspace group membership moved before the delete committed, retrying against its current group")

// undoTSOKeyspaceGroupMembership removes keyspaceID from whatever TSO
// keyspace group it currently belongs to.
//
// It first tries knownGroupID/knownUserKind - the group the sealed
// keyspace's own Config[TSOKeyspaceGroupIDKey] named as of
// sealKeyspaceForRollback's reload. tryGroup's delete loads that one group
// fresh from storage as part of its own transaction, so this attempt needs
// no cache lookup to decide whether the delete should happen: a knownGroupID
// no-op is storage-proven, not a cache guess, meaning the keyspace has
// genuinely moved groups since sealing (only a TSO keyspace-group
// split/merge can still do that - see the function comment on
// rollbackCreateKeyspace). Only then does this fall back to
// GetGroupByKeyspaceID - the GroupManager's cache of current membership,
// which can lag storage briefly behind a concurrent creation or another
// rollback's own group-op commit (both stage their group-membership change
// in the caller's transaction and update the cache in a separate callback
// afterward - see updateKeyspaceForGroupTxnOp) - re-read on every retry
// attempt and re-verified after a successful-looking delete, since that
// same staleness can just as well make a delete attempt here a silent
// no-op - see errRollbackGroupMembershipMoved. Returns nil if the keyspace
// isn't (or is no longer) in any group.
//
// Every delete only ever commits alongside a same-transaction check that
// keyspaceID still identifies the sealed TOMBSTONE keyspace named
// expectedName - see sealKeyspaceForRollback, which the caller must run
// first. That seal already forecloses a state-driven or config-PATCH-driven
// identity change; this check exists for the one thing sealing does not
// prevent, an unrelated keyspace reusing this id after step 4 of
// rollbackCreateKeyspace deletes the meta - and for defense in depth against
// any future caller of this function that does not seal first.
func (manager *Manager) undoTSOKeyspaceGroupMembership(keyspaceID uint32, expectedName, knownGroupID string, knownUserKind endpoint.UserKind) error {
	if manager.kgm == nil {
		return nil
	}
	identityCheckOp := func(txn kv.Txn) error {
		current, err := manager.store.LoadKeyspaceMeta(txn, keyspaceID)
		if err != nil {
			return err
		}
		if current == nil || current.GetState() != keyspacepb.KeyspaceState_TOMBSTONE || current.GetName() != expectedName {
			return errRollbackTargetChanged
		}
		return nil
	}
	// tryGroup attempts to delete keyspaceID's membership from exactly the
	// group gid, storage-verified within the delete's own transaction.
	// done=true means there is nothing more for the caller to do (either the
	// delete actually removed membership, or a same-transaction check
	// proved there was nothing to remove); done=false with a nil error means
	// the delete committed but a post-check found keyspaceID still
	// somewhere, i.e. gid was not (or no longer) where it lives - the caller
	// should look elsewhere, not retry the same gid.
	tryGroup := func(gid uint32, userKind endpoint.UserKind) (done bool, err error) {
		op, cb, err := manager.kgm.updateKeyspaceForGroupTxnOp(userKind, strconv.FormatUint(uint64(gid), 10), keyspaceID, opDelete)
		if err != nil {
			return false, err
		}
		if op == nil {
			// manager.kgm is nil (classic mode): nothing to undo.
			return true, nil
		}
		// The group to target has been decided; pause here for tests
		// exercising what a concurrent caller can/cannot do to the sealed
		// keyspace or its group membership while this commit is pending.
		failpoint.InjectCall("undoTSOKeyspaceGroupMembershipAfterLookup")
		if err := manager.RunTxn(gid, []txnOp{identityCheckOp, op}); err != nil {
			if err == errRollbackTargetChanged { //nolint:errorlint // fixed sentinel, never wrapped
				return true, nil
			}
			return false, err
		}
		cb(nil)
		// A nil commit error does not yet prove keyspaceID's membership was
		// actually removed: the delete is a silent no-op when keyspaceID
		// was no longer a member of gid by commit time (identityCheckOp
		// only guards the keyspace's own identity, not which group
		// currently lists it as a member), so this could have just
		// "successfully" deleted nothing from a group it had already left.
		// Re-resolve to confirm before declaring success.
		if _, _, checkErr := manager.kgm.GetGroupByKeyspaceID(keyspaceID); checkErr != nil {
			return true, nil
		}
		return false, nil
	}

	if knownGroupID != "" {
		if gid, parseErr := strconv.ParseUint(knownGroupID, 10, 32); parseErr == nil {
			if done, err := tryGroup(uint32(gid), knownUserKind); err == nil && done {
				return nil
			}
			// Either an unexpected error (e.g. the known group is mid
			// split/merge) or a storage-proven no-op: either way, fall
			// through to the cache-based search below rather than retrying
			// a group already shown not to have it.
		}
	}

	var err error
	backoff := time.Second
	for i := range 3 {
		var groupID uint32
		var userKind endpoint.UserKind
		if groupID, userKind, err = manager.kgm.GetGroupByKeyspaceID(keyspaceID); err != nil {
			// Not currently in any group - nothing to undo.
			return nil
		}
		var done bool
		if done, err = tryGroup(groupID, userKind); err == nil {
			if done {
				return nil
			}
			err = errRollbackGroupMembershipMoved
		}
		if i < 2 {
			// manager.ctx, not the caller's request context: this cleanup
			// must run to completion even if the original CreateKeyspace
			// call's context is cancelled (e.g. the client disconnected),
			// but should stop promptly on server shutdown instead of
			// blocking it for the rest of the backoff.
			select {
			case <-time.After(backoff):
			case <-manager.ctx.Done():
				return manager.ctx.Err()
			}
			backoff *= 2
		}
	}
	return err
}

// runCreateKeyspaceTxn runs the keyspace creation operations in a single
// transaction. When the keyspace is being assigned to a meta-service group, the
// meta-service group manager's read lock is held for the whole transaction so a
// concurrent group deletion (which takes the write lock) cannot race with the
// assignment.
func (manager *Manager) runCreateKeyspaceTxn(holdMetaGroupLock bool, groupID uint32, ops []txnOp) error {
	if holdMetaGroupLock {
		manager.mgm.RLock()
		defer manager.mgm.RUnlock()
	}
	return manager.RunTxn(groupID, ops)
}

// assignMetaServiceGroupTxnOp returns a txn op that picks the meta-service group
// with the least assigned keyspaces, records the assignment in the keyspace
// config and increments the persisted assignment count, all within the keyspace
// creation transaction. The caller must hold the meta-service group manager's
// read lock for the whole transaction (see runCreateKeyspaceTxn).
func (manager *Manager) assignMetaServiceGroupTxnOp(keyspace *keyspacepb.KeyspaceMeta) (txnOp, txnCb) {
	cb := func(err error) {
		if err != nil {
			return
		}
		manager.mgm.AttachEndpoints(keyspace.Config)
	}
	op := func(txn kv.Txn) error {
		// Re-check under the lock: concurrent PATCHes may remove all groups or
		// leave no enabled group. Create the keyspace without an assignment instead
		// of failing the creation in those cases.
		if !manager.mgm.hasGroupsLocked() {
			return nil
		}
		groupID, err := manager.mgm.findMinMetaGroup(txn)
		if err != nil {
			if errors.Cause(err) == errNoAvailableMetaServiceGroups {
				return nil
			}
			return err
		}
		keyspace.Config[MetaServiceGroupIDKey] = groupID
		return manager.mgm.updateAssignmentTxn(txn, "", groupID)
	}
	return op, cb
}

// RunTxn runs the given operations in a transaction.
// It will serialize the transactions of different keyspaces to avoid deadlock.
func (manager *Manager) RunTxn(groupID uint32, ops []txnOp) error {
	manager.txnLock.Lock(groupID)
	defer manager.txnLock.Unlock(groupID)
	return manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		for _, op := range ops {
			if op == nil {
				continue
			}
			if err := op(txn); err != nil {
				return err
			}
		}
		return nil
	})
}

// CreateKeyspaceByID create a keyspace meta with given config and save it to storage.
// todo: make all etcd operators in one txn to make the operation atomic.
func (manager *Manager) CreateKeyspaceByID(request *CreateKeyspaceByIDRequest) (*keyspacepb.KeyspaceMeta, error) {
	if request.ID == nil {
		return nil, errors.New("keyspace id is empty")
	}
	return manager.createKeyspaceInner(request.Name, request.Config, request.CreateTime, *request.ID)
}

type txnOp = func(txn kv.Txn) error
type txnCb = func(err error)

func (manager *Manager) saveNewKeyspaceTxnOp(keyspace *keyspacepb.KeyspaceMeta) (txnOp, txnCb) {
	cb := func(err error) {
		if err != nil {
			return
		}
		// Re-read from storage instead of storing keyspace.Name/State captured
		// here: this callback runs after txnLock is released (see the callback
		// loop in createKeyspaceWithoutCheck), so it can be delayed behind a
		// concurrent UpdateKeyspaceState(ByID) call that already committed and
		// cached a newer state. See refreshKeyspaceMetaCache.
		manager.refreshKeyspaceMetaCache(keyspace.GetId())
	}
	return func(txn kv.Txn) error {
		// Save keyspace ID.
		// Check if keyspace with that name already exists.
		nameExists, _, err := manager.store.LoadKeyspaceID(txn, keyspace.Name)
		if err != nil {
			return err
		}
		if nameExists {
			return errs.ErrKeyspaceExists
		}
		err = manager.store.SaveKeyspaceID(txn, keyspace.GetId(), keyspace.Name)
		if err != nil {
			return err
		}
		// Save keyspace meta.
		// Check if keyspace with that id already exists.
		loadedMeta, err := manager.store.LoadKeyspaceMeta(txn, keyspace.GetId())
		if err != nil {
			return err
		}
		if loadedMeta != nil {
			return errs.ErrKeyspaceExists
		}
		return manager.store.SaveKeyspaceMeta(txn, keyspace)
	}, cb
}

// saveKeyspaceRegionLabelerTxnOp returns a txn op that adds the keyspace's
// boundaries to the region label. The corresponding region will then be split by
// the Coordinator's patrolRegion. The post-commit callback updates the in-memory
// region labeler so it is only mutated once the label rule is durably persisted.
func (manager *Manager) saveKeyspaceRegionLabelerTxnOp(id uint32, boundType regionBoundType) (txnOp, txnCb, error) {
	failpoint.Inject("skipSplitRegion", func() {
		failpoint.Return(nil, nil, nil)
	})
	keyspaceRule := buildLabelRule(id, boundType)
	cl, ok := manager.cluster.(interface{ GetRegionLabeler() *labeler.RegionLabeler })
	if !ok {
		return nil, nil, errors.New("cluster does not support region label")
	}
	err := keyspaceRule.CheckAndAdjust()
	if err != nil {
		return nil, nil, err
	}
	regionLabeler := cl.GetRegionLabeler()
	cb := func(err error) {
		if err != nil {
			return
		}
		// Re-read the rule from storage instead of caching the keyspaceRule
		// snapshot captured above: this callback can run after a concurrent
		// SetLabelRule/DeleteLabelRule on the same rule ID (nothing reserves
		// the "keyspaces/<id>" namespace against the generic label-rule API)
		// has already durably published a newer value or deleted it, and
		// caching the stale snapshot would silently diverge from storage
		// with no later reconciliation. ReloadRuleFromStorage does the read
		// and the cache update under one lock hold, so this can't itself
		// race a concurrent writer the way a separate read-then-write pair
		// would.
		found, reloadErr := regionLabeler.ReloadRuleFromStorage(keyspaceRule.ID)
		if reloadErr != nil {
			log.Error("failed to reload region label rule into cache after commit, cache may be stale",
				zap.Uint32("keyspace-id", id), zap.String("rule-id", keyspaceRule.ID), errs.ZapError(reloadErr))
			return
		}
		if !found {
			log.Warn("region label rule was deleted by a concurrent writer before it could be cached",
				zap.Uint32("keyspace-id", id), zap.String("rule-id", keyspaceRule.ID))
			return
		}
		log.Info("added region label for keyspace",
			zap.Uint32("keyspace-id", id),
			zap.Any("label-rule", keyspaceRule),
			zap.Stringer("key-type", boundType),
		)
	}
	return func(txn kv.Txn) error {
		return regionLabeler.GetRuleStorage().SaveRegionRule(txn, keyspaceRule.ID, keyspaceRule)
	}, cb, nil
}

func (manager *Manager) waitKeyspaceRegionSplit(id uint32, boundType regionBoundType) error {
	ticker := time.NewTicker(manager.config.GetCheckRegionSplitInterval())
	timer := time.NewTimer(manager.config.GetWaitRegionSplitTimeout())
	defer func() {
		ticker.Stop()
		timer.Stop()
	}()
	for {
		select {
		case <-manager.ctx.Done():
			return errors.New("[keyspace] wait region split canceled")
		case <-ticker.C:
			if manager.hasKeyspaceRegionBound(id, boundType) {
				log.Info("[keyspace] wait region split successfully", zap.Uint32("keyspace-id", id))
				return nil
			}
			// Note: we reset the ticker here to support updating configuration dynamically.
			ticker.Reset(manager.config.GetCheckRegionSplitInterval())
		case <-timer.C:
			err := errs.ErrRegionSplitTimeout
			return err
		}
	}
}

// CheckKeyspaceRegionBound checks whether the keyspace region has been split.
func (manager *Manager) CheckKeyspaceRegionBound(meta *keyspacepb.KeyspaceMeta) bool {
	// Only an ENABLED keyspace can be considered region-bound-ready: once the
	// keyspace is DISABLED/ARCHIVED/TOMBSTONE - regardless of how it left
	// ENABLED - state alone governs whether it is usable, and the sole
	// caller (LoadKeyspace) relies on this to decide whether the keyspace
	// should still be visible.
	if meta.GetState() != keyspacepb.KeyspaceState_ENABLED {
		return false
	}
	// wait_region_split only controls whether creation blocks waiting for the
	// split synchronously - it says nothing about whether the region has
	// actually been split yet. A keyspace created with wait_region_split
	// "false" still needs its region split (asynchronously, by the
	// Coordinator's patrolRegion - see saveKeyspaceRegionLabelerTxnOp)
	// before it is safe to use: TestCreateClientWithKeyspaceCheck reproduces
	// this under the nextgen tag, where the auto-created system keyspace has
	// wait_region_split "false" and is ENABLED immediately, but its region
	// is not actually split until the Coordinator gets to it. Always check
	// the real bound; never shortcut on wait_region_split alone.
	config := meta.GetConfig()
	val, ok := config[RegionBoundType]
	// if config does not contain region bound type, we use the default one from manager.
	if !ok {
		val = manager.getRegionBoundType().String()
	}
	typ := keyTypeStringToRegionBoundType(val)
	return manager.hasKeyspaceRegionBound(meta.GetId(), typ)
}

func (manager *Manager) hasKeyspaceRegionBound(id uint32, boundType regionBoundType) bool {
	failpoint.Inject("skipSplitRegion", func() {
		failpoint.Return(true)
	})
	regionBound := MakeRegionBound(id)
	if boundType == txnRegionBound {
		return manager.checkBound(regionBound.TxnLeftBound) &&
			manager.checkBound(regionBound.TxnRightBound)
	}
	return manager.checkBound(regionBound.RawLeftBound) && manager.checkBound(regionBound.RawRightBound)
}

func (manager *Manager) getRegionBoundType() regionBoundType {
	if manager.cluster == nil || manager.cluster.GetSharedConfig() == nil {
		return txnRegionBound
	}
	return keyTypeToRegionBoundType(manager.cluster.GetSharedConfig().GetKeyType())
}

func (manager *Manager) checkBound(key []byte) bool {
	if manager.cluster == nil {
		return false
	}
	c := manager.cluster.GetBasicCluster()
	region := c.GetRegionByKey(key)
	if region == nil || !bytes.Equal(region.GetStartKey(), key) {
		return false
	}
	return true
}

// LoadKeyspace returns the keyspace specified by name.
// It returns error if loading or unmarshalling met error or if keyspace does not exist.
func (manager *Manager) LoadKeyspace(name string) (*keyspacepb.KeyspaceMeta, error) {
	var meta *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		loaded, id, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil {
			return err
		}
		if !loaded {
			return errs.ErrKeyspaceNotFound
		}
		meta, err = manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if meta == nil {
			return errs.ErrKeyspaceNotFound
		}
		return nil
	})
	if manager.mgm != nil && meta != nil {
		manager.mgm.AttachEndpoints(meta.GetConfig())
	}
	return meta, err
}

// LoadKeyspaceByID returns the keyspace specified by id.
// It returns error if loading or unmarshalling met error or if keyspace does not exist.
func (manager *Manager) LoadKeyspaceByID(spaceID uint32) (*keyspacepb.KeyspaceMeta, error) {
	var (
		meta *keyspacepb.KeyspaceMeta
		err  error
	)
	err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		meta, err = manager.store.LoadKeyspaceMeta(txn, spaceID)
		if err != nil {
			return err
		}
		if meta == nil {
			return errs.ErrKeyspaceNotFound
		}
		return nil
	})
	if manager.mgm != nil && meta != nil {
		manager.mgm.AttachEndpoints(meta.GetConfig())
	}
	return meta, err
}

// Mutation represents a single operation to be applied on keyspace config.
type Mutation struct {
	Op    OpType
	Key   string
	Value string
}

// OpType defines the type of keyspace config operation.
type OpType int

const (
	// OpPut denotes a put operation onto the given config.
	// If target key exists, it will put a new value,
	// otherwise, it creates a new config entry.
	OpPut OpType = iota + 1 // Operation type starts at 1.
	// OpDel denotes a deletion operation onto the given config.
	// Note: OpDel is idempotent, deleting a non-existing key
	// will not result in error.
	OpDel
)

// UpdateKeyspaceConfig changes target keyspace's config in the order specified in mutations.
// It returns error if saving failed, operation not allowed, or if keyspace not exists.
func (manager *Manager) UpdateKeyspaceConfig(name string, mutations []*Mutation) (*keyspacepb.KeyspaceMeta, error) {
	return manager.updateKeyspaceConfigTxn(name, func(meta *keyspacepb.KeyspaceMeta) error {
		return applyKeyspaceConfigMutations(meta.Config, mutations)
	})
}

// UpdateKeyspaceConfigWithPreconditions changes target keyspace's config in the order specified in mutations if the
// given preconditions are satisfied.
// Preconditions use a JSON-merge-patch-like encoding:
// - key -> null means the key must be absent.
// - key -> "value" means the key must exist and equal "value".
func (manager *Manager) UpdateKeyspaceConfigWithPreconditions(name string, mutations []*Mutation, preconditions map[string]*string) (*keyspacepb.KeyspaceMeta, error) {
	if len(preconditions) == 0 {
		return manager.UpdateKeyspaceConfig(name, mutations)
	}
	return manager.updateKeyspaceConfigTxn(name, func(meta *keyspacepb.KeyspaceMeta) error {
		if err := checkKeyspaceConfigPreconditions(meta.GetConfig(), preconditions); err != nil {
			return err
		}
		return applyKeyspaceConfigMutations(meta.Config, mutations)
	})
}

func checkKeyspaceConfigPreconditions(config map[string]string, preconditions map[string]*string) error {
	for k, expected := range preconditions {
		actual, exists := config[k]
		if expected == nil {
			if exists {
				return errs.ErrKeyspaceConfigPreconditionFailed.FastGenByArgs(k + " must be absent")
			}
			continue
		}
		if !exists {
			return errs.ErrKeyspaceConfigPreconditionFailed.FastGenByArgs(k + " does not exist")
		}
		if actual != *expected {
			return errs.ErrKeyspaceConfigPreconditionFailed.FastGenByArgs("key=" + k + " expected=" + *expected + " actual=" + actual)
		}
	}
	return nil
}

func applyKeyspaceConfigMutations(config map[string]string, mutations []*Mutation) error {
	for _, mutation := range mutations {
		switch mutation.Op {
		case OpPut:
			config[mutation.Key] = mutation.Value
		case OpDel:
			delete(config, mutation.Key)
		default:
			return errs.ErrIllegalOperation
		}
	}
	return nil
}

// runTxnWithMetaGroupLock runs f inside a storage transaction while holding the
// meta-service group manager's read lock for the whole transaction. This keeps
// keyspace assignment validation and the persisted assignment count update
// atomic with respect to MetaServiceGroupManager.UpdateGroupsSafely, which takes
// the write lock before deleting a group.
func (manager *Manager) runTxnWithMetaGroupLock(f func(txn kv.Txn) error) error {
	if manager.mgm != nil {
		manager.mgm.RLock()
		defer manager.mgm.RUnlock()
	}
	return manager.store.RunInTxn(manager.ctx, f)
}

func (manager *Manager) updateKeyspaceConfigTxn(name string, update func(meta *keyspacepb.KeyspaceMeta) error) (*keyspacepb.KeyspaceMeta, error) {
	var meta *keyspacepb.KeyspaceMeta
	oldConfig := make(map[string]string)
	txnFunc := func(txn kv.Txn) error {
		// First get KeyspaceID from Name.
		loaded, id, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil {
			return err
		}
		if !loaded {
			return errs.ErrKeyspaceNotFound
		}
		manager.metaLock.Lock(id)
		defer manager.metaLock.Unlock(id)
		// Load keyspace by id.
		meta, err = manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if meta == nil {
			return errs.ErrKeyspaceNotFound
		}
		// Only keyspace with state listed in allowChangeConfig are allowed to change their config.
		if !slice.Contains(allowChangeConfig, meta.GetState()) {
			return errors.Errorf("cannot change config for keyspace with state %s", meta.GetState().String())
		}
		// Initialize meta's config map if it's nil.
		if meta.GetConfig() == nil {
			meta.Config = map[string]string{}
		}
		for k, v := range meta.GetConfig() {
			oldConfig[k] = v
		}
		// Update keyspace config.
		if err := update(meta); err != nil {
			return err
		}
		delete(meta.Config, MetaServiceGroupAddressesKey)
		newConfig := meta.GetConfig()
		// Reassign the meta-service group before moving the TSO keyspace group.
		// reassignKeyspaceLocked only stages its changes in txn (discarded if the
		// txn doesn't commit), while UpdateKeyspaceGroup persists immediately. Doing
		// the fallible meta-service validation first avoids leaving the TSO group move
		// persisted but unreverted when the meta-service reassignment fails.
		oldMetaServiceGroup := oldConfig[MetaServiceGroupIDKey]
		newMetaServiceGroup := newConfig[MetaServiceGroupIDKey]
		if manager.mgm != nil && oldMetaServiceGroup != newMetaServiceGroup {
			// The read lock held by runTxnWithMetaGroupLock keeps this validation and
			// the assignment update atomic with respect to UpdateGroupsSafely.
			if err := manager.mgm.reAssignKeyspaceLocked(txn, oldMetaServiceGroup, newMetaServiceGroup); err != nil {
				return err
			}
		}
		oldUserKind := endpoint.StringUserKind(oldConfig[UserKindKey])
		newUserKind := endpoint.StringUserKind(newConfig[UserKindKey])
		oldID := oldConfig[TSOKeyspaceGroupIDKey]
		newID := newConfig[TSOKeyspaceGroupIDKey]
		needUpdate := oldUserKind != newUserKind || oldID != newID
		if needUpdate {
			if err := manager.kgm.UpdateKeyspaceGroup(oldID, newID, oldUserKind, newUserKind, meta.GetId()); err != nil {
				return err
			}
		}
		// Save the updated keyspace meta.
		if err := manager.store.SaveKeyspaceMeta(txn, meta); err != nil {
			if needUpdate {
				if err := manager.kgm.UpdateKeyspaceGroup(newID, oldID, newUserKind, oldUserKind, meta.GetId()); err != nil {
					log.Error("failed to revert keyspace group", zap.Error(err))
				}
			}
			return err
		}
		return nil
	}
	err := manager.runTxnWithMetaGroupLock(txnFunc)
	if err != nil {
		log.Warn("[keyspace] failed to update keyspace config",
			zap.Uint32("keyspace-id", meta.GetId()),
			zap.String("name", meta.GetName()),
			zap.Error(err),
		)
		return nil, err
	}
	if manager.mgm != nil {
		manager.mgm.AttachEndpoints(meta.GetConfig())
	}
	log.Info("[keyspace] keyspace config updated",
		zap.Uint32("keyspace-id", meta.GetId()),
		zap.String("name", meta.GetName()),
		zap.Any("new-config", meta.GetConfig()),
	)
	return meta, nil
}

// UpdateKeyspaceState updates target keyspace to the given state if it's not already in that state.
// It returns error if saving failed, operation not allowed, or if keyspace not exists.
func (manager *Manager) UpdateKeyspaceState(name string, newState keyspacepb.KeyspaceState, now int64) (*keyspacepb.KeyspaceMeta, error) {
	if isProtectedKeyspaceName(name) {
		err := newModifyProtectedKeyspaceError()
		log.Warn("[keyspace] failed to update keyspace config", errs.ZapError(err))
		return nil, err
	}
	var meta *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		// First get KeyspaceID from Name.
		loaded, id, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil {
			return err
		}
		if !loaded {
			return errs.ErrKeyspaceNotFound
		}
		manager.metaLock.Lock(id)
		defer manager.metaLock.Unlock(id)
		// Load keyspace by id.
		meta, err = manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if meta == nil {
			return errs.ErrKeyspaceNotFound
		}
		// Update keyspace meta.
		if err = manager.transformKeyspaceState(txn, meta, newState, now); err != nil {
			return err
		}
		return manager.store.SaveKeyspaceMeta(txn, meta)
	})
	if err != nil {
		log.Warn("[keyspace] failed to update keyspace config",
			zap.Uint32("keyspace-id", meta.GetId()),
			zap.String("name", meta.GetName()),
			zap.Error(err),
		)
		return nil, err
	}
	manager.refreshKeyspaceMetaCache(meta.GetId())
	if manager.mgm != nil {
		manager.mgm.AttachEndpoints(meta.GetConfig())
	}
	log.Info("[keyspace] keyspace state updated",
		zap.Uint32("id", meta.GetId()),
		zap.String("keyspace-id", meta.GetName()),
		zap.String("new-state", newState.String()),
	)
	return meta, nil
}

// refreshKeyspaceMetaCache re-reads the current durable keyspace meta and
// syncs keyspaceNameLookup/keyspaceStateLookup to it under metaLock, instead
// of writing a value the caller computed or captured earlier. A post-commit
// cache update can be delayed arbitrarily (see runCreateKeyspaceTxn's
// callback loop, which runs after txnLock is released); writing a caller-
// captured value directly can let a slow, stale callback clobber a cache
// entry a faster, later commit already refreshed. Re-reading under metaLock
// instead means whichever call reaches this last always reads what is
// currently durable, so the cache converges to storage regardless of
// callback ordering. Callers must not hold metaLock for id when calling
// this. If the reload itself fails, the cache entries are invalidated
// (deleted) rather than left as-is: GetKeyspaceNameByID/GetKeyspaceStateByID
// only fall back to storage on a cache *miss*, so leaving a stale-but-present
// entry in place would make them trust a value that's known to potentially
// be wrong - deleting it forces the next read through that safe fallback
// instead.
func (manager *Manager) refreshKeyspaceMetaCache(id uint32) {
	manager.metaLock.Lock(id)
	defer manager.metaLock.Unlock(id)
	var meta *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		var err error
		meta, err = manager.store.LoadKeyspaceMeta(txn, id)
		return err
	})
	if err != nil {
		log.Warn("[keyspace] failed to refresh keyspace cache", zap.Uint32("keyspace-id", id), errs.ZapError(err))
		manager.keyspaceNameLookup.Delete(id)
		manager.keyspaceStateLookup.Delete(id)
		return
	}
	if meta == nil {
		manager.keyspaceNameLookup.Delete(id)
		manager.keyspaceStateLookup.Delete(id)
		return
	}
	manager.keyspaceNameLookup.Store(id, meta.GetName())
	manager.keyspaceStateLookup.Store(id, meta.GetState())
}

// RemoveKeyspace removes the keyspace specified by id if it's in proper state and not protected.
func (manager *Manager) RemoveKeyspace(txn kv.Txn, id uint32) error {
	manager.metaLock.Lock(id)
	defer manager.metaLock.Unlock(id)
	if isProtectedKeyspaceID(id) {
		return newModifyProtectedKeyspaceError()
	}
	meta, err := manager.store.LoadKeyspaceMeta(txn, id)
	if err != nil {
		return err
	}
	if meta == nil {
		return errs.ErrKeyspaceNotFound
	}
	if meta.GetState() == keyspacepb.KeyspaceState_ENABLED || meta.GetState() == keyspacepb.KeyspaceState_DISABLED {
		return errors.Errorf("cannot remove keyspace in state %s", meta.GetState().String())
	}
	err = manager.store.RemoveKeyspace(txn, id, meta.GetName())
	if err != nil {
		return err
	}
	// The cache is deliberately not touched here: txn belongs to the caller
	// and may still be rolled back (RemoveKeyspacesFromGroup batches several
	// of these into one transaction), so mutating the cache now could make it
	// show a removal that storage later reverts. The caller must refresh the
	// cache (see refreshKeyspaceMetaCache) once its transaction has actually
	// committed.
	//
	// Keep the meta-service group assignment accounting in sync within the same
	// txn. Without this, removed keyspaces leak count and could permanently block
	// deleting an otherwise-empty group.
	return manager.unassignKeyspaceFromMetaServiceGroup(txn, meta)
}

// UpdateKeyspaceStateByID updates target keyspace to the given state if it's not already in that state.
// It returns error if saving failed, operation not allowed, or if keyspace not exists.
func (manager *Manager) UpdateKeyspaceStateByID(id uint32, newState keyspacepb.KeyspaceState, now int64) (*keyspacepb.KeyspaceMeta, error) {
	if isProtectedKeyspaceID(id) {
		err := newModifyProtectedKeyspaceError()
		log.Warn("[keyspace] failed to update keyspace config", errs.ZapError(err))
		return nil, err
	}
	var meta *keyspacepb.KeyspaceMeta
	var err error
	err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		manager.metaLock.Lock(id)
		defer manager.metaLock.Unlock(id)
		// Load keyspace by id.
		meta, err = manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if meta == nil {
			return errs.ErrKeyspaceNotFound
		}
		// Update keyspace meta.
		if err = manager.transformKeyspaceState(txn, meta, newState, now); err != nil {
			return err
		}
		failpoint.Inject("saveKeyspaceMetaFailed", func() {
			err = errors.New("failpoint triggered: saveKeyspaceMetaFailed")
		})
		if err != nil {
			return err
		}
		return manager.store.SaveKeyspaceMeta(txn, meta)
	})
	if err != nil {
		log.Warn("[keyspace] failed to update keyspace config",
			zap.Uint32("keyspace-id", meta.GetId()),
			zap.String("name", meta.GetName()),
			zap.Error(err),
		)
		return nil, err
	}
	manager.refreshKeyspaceMetaCache(meta.GetId())
	log.Info("[keyspace] keyspace state updated",
		zap.Uint32("keyspace-id", meta.GetId()),
		zap.String("name", meta.GetName()),
		zap.String("new-state", newState.String()),
	)
	if manager.mgm != nil {
		manager.mgm.AttachEndpoints(meta.GetConfig())
	}
	return meta, nil
}

func (manager *Manager) enableNewKeyspace(id uint32, now int64) error {
	var meta *keyspacepb.KeyspaceMeta
	var err error
	err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		manager.metaLock.Lock(id)
		defer manager.metaLock.Unlock(id)
		meta, err = manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if meta == nil {
			return errs.ErrKeyspaceNotFound
		}
		if err = manager.transformKeyspaceState(txn, meta, keyspacepb.KeyspaceState_ENABLED, now); err != nil {
			return err
		}
		return manager.store.SaveKeyspaceMeta(txn, meta)
	})
	if err != nil {
		return err
	}
	manager.refreshKeyspaceMetaCache(meta.GetId())
	if manager.mgm != nil {
		manager.mgm.AttachEndpoints(meta.GetConfig())
	}
	return nil
}

// unassignKeyspaceFromMetaServiceGroup removes the keyspace's meta-service group
// binding within txn: it drops MetaServiceGroupIDKey from the config and, when a
// meta-service group manager is configured, decrements the persisted assignment
// count. Once the config key is removed and persisted, a subsequent call is a
// no-op, so the removal and tombstone paths can both invoke it without
// double-counting.
//
// Callers already hold the keyspace metaLock (so meta is not mutated
// concurrently). This deliberately does NOT take mgm.RLock: the config-update
// path acquires mgm.RLock before metaLock (via runTxnWithMetaGroupLock), so
// grabbing mgm.RLock here while holding metaLock would invert the lock order and
// deadlock once UpdateGroupsSafely is waiting on mgm.Lock. The lock is
// unnecessary anyway — updateAssignmentTxn only touches the store, and the
// group delete guard relies on the authoritative keyspace scan, not this count.
func (manager *Manager) unassignKeyspaceFromMetaServiceGroup(txn kv.Txn, meta *keyspacepb.KeyspaceMeta) error {
	groupID := meta.GetConfig()[MetaServiceGroupIDKey]
	if groupID == "" {
		return nil
	}
	delete(meta.Config, MetaServiceGroupIDKey)
	if manager.mgm == nil {
		return nil
	}
	return manager.mgm.updateAssignmentTxn(txn, groupID, "")
}

// transformKeyspaceState transforms the keyspace state to the target state and record the update time.
func (manager *Manager) transformKeyspaceState(txn kv.Txn, meta *keyspacepb.KeyspaceMeta, newState keyspacepb.KeyspaceState, now int64) error {
	// If already in the target state, do nothing and return. A TOMBSTONE keyspace
	// still carrying a meta-service group binding (e.g. one tombstoned before this
	// cleanup existed) is repaired here by re-applying the TOMBSTONE update; the
	// unassignment is idempotent, so it is a no-op once the binding is cleared.
	if meta.GetState() == newState {
		if newState == keyspacepb.KeyspaceState_TOMBSTONE {
			return manager.unassignKeyspaceFromMetaServiceGroup(txn, meta)
		}
		return nil
	}
	// Consult state transition table to check if the operation is legal.
	if !slice.Contains(stateTransitionTable[meta.GetState()], newState) {
		return errors.Errorf("cannot change keyspace state from %s to %s", meta.GetState().String(), newState.String())
	}
	if newState == keyspacepb.KeyspaceState_TOMBSTONE {
		if err := manager.unassignKeyspaceFromMetaServiceGroup(txn, meta); err != nil {
			return err
		}
	}
	// If the operation is legal, update keyspace state and change time. The
	// keyspaceStateLookup cache is deliberately not updated here: this runs
	// inside the caller's transaction closure, before the transaction is known
	// to have committed. Callers update the cache themselves once RunInTxn
	// returns successfully, so a failed commit cannot leave the cache showing a
	// state that was never durably persisted.
	meta.State = newState
	meta.StateChangedAt = now
	return nil
}

// LoadRangeKeyspace load up to limit keyspaces starting from keyspace with startID.
// It will not load the NullKeyspace meta data.
func (manager *Manager) LoadRangeKeyspace(startID uint32, limit int) ([]*keyspacepb.KeyspaceMeta, error) {
	// Load Start should fall within acceptable ID range.
	if startID > constant.MaxValidKeyspaceID {
		return nil, errors.Errorf("startID of the scan %d exceeds spaceID Max %d", startID, constant.MaxValidKeyspaceID)
	}
	var (
		keyspaces []*keyspacepb.KeyspaceMeta
		err       error
	)
	err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		keyspaces, err = manager.store.LoadRangeKeyspace(txn, startID, limit)
		return err
	})
	if err != nil {
		return nil, err
	}
	if manager.mgm != nil {
		for _, meta := range keyspaces {
			if meta != nil {
				manager.mgm.AttachEndpoints(meta.GetConfig())
			}
		}
	}
	return keyspaces, nil
}

// CountKeyspacesByMetaServiceGroup scans all keyspaces and counts how many are
// currently assigned to each of the given meta-service groups. It is intended
// only for the meta-service group delete guard, because it may scan many
// keyspace metadata records from etcd. Do not use it on regular request paths.
// A stale assignment counter must never permanently block removing a group that
// has no keyspaces actually referencing it.
func (manager *Manager) CountKeyspacesByMetaServiceGroup(groupIDs []string) (map[string]int, error) {
	counts := make(map[string]int, len(groupIDs))
	if len(groupIDs) == 0 {
		return counts, nil
	}
	groupSet := make(map[string]struct{}, len(groupIDs))
	for _, groupID := range groupIDs {
		counts[groupID] = 0
		groupSet[groupID] = struct{}{}
	}
	startID := constant.StartKeyspaceID
	for {
		// Load directly from the store rather than via LoadRangeKeyspace: the
		// latter calls mgm.AttachEndpoints which takes the mgm read lock, and this
		// is invoked while the mgm write lock is held, which would deadlock.
		var keyspaces []*keyspacepb.KeyspaceMeta
		if err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
			var err error
			keyspaces, err = manager.store.LoadRangeKeyspace(txn, startID, etcdutil.MaxEtcdTxnOps)
			return err
		}); err != nil {
			return nil, err
		}
		for _, ks := range keyspaces {
			if ks == nil {
				continue
			}
			groupID := ks.GetConfig()[MetaServiceGroupIDKey]
			if groupID == "" {
				continue
			}
			if _, ok := groupSet[groupID]; ok {
				counts[groupID]++
			}
		}
		if len(keyspaces) < etcdutil.MaxEtcdTxnOps {
			break
		}
		startID = keyspaces[len(keyspaces)-1].GetId() + 1
	}
	return counts, nil
}

// GetKeyspaceNameByID gets the keyspace name by ID, which will try to get it from the cache first.
// If not found, it will try to get it from the storage.
func (manager *Manager) GetKeyspaceNameByID(id uint32) (string, error) {
	if id == constant.NullKeyspaceID {
		return "", nil
	}
	// Try to get the keyspace name from the cache first.
	name, ok := manager.keyspaceNameLookup.Load(id)
	if ok {
		return name.(string), nil
	}
	var loadedName string
	// If the keyspace name is not in the cache, try to get it from the storage.
	meta, err := manager.LoadKeyspaceByID(id)
	if err != nil {
		return "", err
	}
	loadedName = meta.GetName()
	if len(loadedName) == 0 {
		return "", errors.Errorf("got an empty keyspace name by id %d", id)
	}
	// Load or store the keyspace name to the cache.
	actual, _ := manager.keyspaceNameLookup.LoadOrStore(id, loadedName)
	return actual.(string), nil
}

// GetKeyspaceStateByID gets the keyspace state by ID, which will try to get it from the cache first.
// If not found, it will try to get it from the storage.
func (manager *Manager) GetKeyspaceStateByID(id uint32) (keyspacepb.KeyspaceState, error) {
	if id == constant.NullKeyspaceID {
		return keyspacepb.KeyspaceState_DISABLED, nil
	}
	state, ok := manager.keyspaceStateLookup.Load(id)
	if ok {
		return state.(keyspacepb.KeyspaceState), nil
	}
	var loadedState keyspacepb.KeyspaceState
	// If the keyspace state is not in the cache, try to get it from the storage.
	meta, err := manager.LoadKeyspaceByID(id)
	// Only check wether the keyspace meta is nil, ensure the returned state is as latest as possible.
	if meta == nil {
		return keyspacepb.KeyspaceState_DISABLED, err
	}
	loadedState = meta.GetState()
	// Load or store the keyspace state to the cache.
	actual, _ := manager.keyspaceStateLookup.LoadOrStore(id, loadedState)
	return actual.(keyspacepb.KeyspaceState), nil
}

// GetEnabledKeyspaceNameByID gets the enabled keyspace name by ID. If the state is not enabled, it will return an error.
// This method is useful for getting the keyspace name while checking whether the keyspace is enabled.
func (manager *Manager) GetEnabledKeyspaceNameByID(id uint32) (string, error) {
	state, err := manager.GetKeyspaceStateByID(id)
	if err != nil {
		return "", err
	}
	if state != keyspacepb.KeyspaceState_ENABLED {
		return "", errors.Errorf("keyspace %d is not enabled, current state is %s", id, state.String())
	}
	return manager.GetKeyspaceNameByID(id)
}

// IterateKeyspaces returns an iterator that yields all keyspaces starting from startID.
// In case the keyspaces are being modified while iteration is in progress, it's not guaranteed that the results are
// in a consistent snapshot.
func (manager *Manager) IterateKeyspaces() *Iterator {
	return newKeyspaceIterator(manager)
}

// allocID allocate a new keyspace id.
func (manager *Manager) allocID() (uint32, error) {
	id64, _, err := manager.idAllocator.Alloc(1)
	if err != nil {
		return 0, err
	}
	id32 := uint32(id64)
	if err = validateID(id32); err != nil {
		return 0, err
	}
	return id32, nil
}

// PatrolKeyspaceAssignment is used to patrol all keyspaces and assign them to the keyspace groups.
func (manager *Manager) PatrolKeyspaceAssignment(startKeyspaceID, endKeyspaceID uint32) error {
	if startKeyspaceID > manager.nextPatrolStartID {
		manager.nextPatrolStartID = startKeyspaceID
	}
	if endKeyspaceID != 0 && endKeyspaceID < manager.nextPatrolStartID {
		log.Info("[keyspace] end keyspace id is smaller than the next patrol start id, skip patrol",
			zap.Uint32("end-keyspace-id", endKeyspaceID),
			zap.Uint32("next-patrol-start-id", manager.nextPatrolStartID))
		return nil
	}
	var (
		// Some statistics info.
		start                  = time.Now()
		patrolledKeyspaceCount uint64
		assignedKeyspaceCount  uint64
		// The current start ID of the patrol, used for logging.
		currentStartID = manager.nextPatrolStartID
		// The next start ID of the patrol, used for the next patrol.
		nextStartID  = currentStartID
		moreToPatrol = true
		err          error
	)
	defer func() {
		log.Debug("[keyspace] patrol keyspace assignment finished",
			zap.Duration("cost", time.Since(start)),
			zap.Uint64("patrolled-keyspace-count", patrolledKeyspaceCount),
			zap.Uint64("assigned-keyspace-count", assignedKeyspaceCount),
			zap.Int("batch-size", etcdutil.MaxEtcdTxnOps),
			zap.Uint32("start-keyspace-id", startKeyspaceID),
			zap.Uint32("end-keyspace-id", endKeyspaceID),
			zap.Uint32("current-start-id", currentStartID),
			zap.Uint32("next-start-id", nextStartID),
		)
	}()
	for moreToPatrol {
		var defaultKeyspaceGroup *endpoint.KeyspaceGroup
		err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
			var err error
			defaultKeyspaceGroup, err = manager.kgm.store.LoadKeyspaceGroup(txn, constant.DefaultKeyspaceGroupID)
			if err != nil {
				return err
			}
			if defaultKeyspaceGroup == nil {
				return errors.Errorf("default keyspace group %d not found", constant.DefaultKeyspaceGroupID)
			}
			if defaultKeyspaceGroup.IsSplitting() {
				return errs.ErrKeyspaceGroupInSplit.FastGenByArgs(constant.DefaultKeyspaceGroupID)
			}
			if defaultKeyspaceGroup.IsMerging() {
				return errs.ErrKeyspaceGroupInMerging.FastGenByArgs(constant.DefaultKeyspaceGroupID)
			}
			keyspaces, err := manager.store.LoadRangeKeyspace(txn, manager.nextPatrolStartID, etcdutil.MaxEtcdTxnOps)
			if err != nil {
				return err
			}
			keyspaceNum := len(keyspaces)
			// If there are more than one keyspace, update the current and next start IDs.
			if keyspaceNum > 0 {
				currentStartID = keyspaces[0].GetId()
				nextStartID = keyspaces[keyspaceNum-1].GetId() + 1
			}
			// If there are less than ` etcdutil.MaxEtcdTxnOps` keyspaces or the next start ID reaches the end,
			// there is no need to patrol again.
			moreToPatrol = keyspaceNum == etcdutil.MaxEtcdTxnOps
			var (
				assigned            = false
				keyspaceIDsToUnlock = make([]uint32, 0, keyspaceNum)
			)
			defer func() {
				for _, id := range keyspaceIDsToUnlock {
					manager.metaLock.Unlock(id)
				}
			}()
			for _, ks := range keyspaces {
				if ks == nil {
					continue
				}
				if endKeyspaceID != 0 && ks.GetId() > endKeyspaceID {
					moreToPatrol = false
					break
				}
				patrolledKeyspaceCount++
				manager.metaLock.Lock(ks.GetId())
				if ks.Config == nil {
					ks.Config = make(map[string]string, 1)
				} else if _, ok := ks.Config[TSOKeyspaceGroupIDKey]; ok {
					// If the keyspace already has a group ID, skip it.
					manager.metaLock.Unlock(ks.GetId())
					continue
				}
				// Unlock the keyspace meta lock after the whole txn.
				keyspaceIDsToUnlock = append(keyspaceIDsToUnlock, ks.GetId())
				// If the keyspace doesn't have a group ID, assign it to the default keyspace group.
				if !slice.Contains(defaultKeyspaceGroup.Keyspaces, ks.GetId()) {
					defaultKeyspaceGroup.Keyspaces = append(defaultKeyspaceGroup.Keyspaces, ks.GetId())
					// Only save the keyspace group meta if any keyspace is assigned to it.
					assigned = true
				}
				ks.Config[TSOKeyspaceGroupIDKey] = strconv.FormatUint(uint64(constant.DefaultKeyspaceGroupID), 10)
				err = manager.store.SaveKeyspaceMeta(txn, ks)
				if err != nil {
					log.Error("[keyspace] failed to save keyspace meta during patrol",
						zap.Int("batch-size", etcdutil.MaxEtcdTxnOps),
						zap.Uint32("start-keyspace-id", startKeyspaceID),
						zap.Uint32("end-keyspace-id", endKeyspaceID),
						zap.Uint32("current-start-id", currentStartID),
						zap.Uint32("next-start-id", nextStartID),
						zap.Uint32("keyspace-id", ks.GetId()), zap.Error(err))
					return err
				}
				assignedKeyspaceCount++
			}
			if assigned {
				err = manager.kgm.store.SaveKeyspaceGroup(txn, defaultKeyspaceGroup)
				if err != nil {
					log.Error("[keyspace] failed to save default keyspace group meta during patrol",
						zap.Int("batch-size", etcdutil.MaxEtcdTxnOps),
						zap.Uint32("start-keyspace-id", startKeyspaceID),
						zap.Uint32("end-keyspace-id", endKeyspaceID),
						zap.Uint32("current-start-id", currentStartID),
						zap.Uint32("next-start-id", nextStartID), zap.Error(err))
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		manager.kgm.Lock()
		manager.kgm.putKeyspaceGroupToCacheLocked(defaultKeyspaceGroup)
		manager.kgm.Unlock()
		// If all keyspaces in the current batch are assigned, update the next start ID.
		manager.nextPatrolStartID = nextStartID
	}
	return nil
}

// IteratorLoadingBatchSize is the batch size that the keyspace.Iterator internally loads keyspaces.
// This constant is public for test purposes.
const IteratorLoadingBatchSize int = 100

// Iterator iterates over all keyspaces.
// Create this using keyspace.Manager.IterateKeyspaces, and use Next method for iteration.
type Iterator struct {
	manager      *Manager
	currentBatch []*keyspacepb.KeyspaceMeta
	currentIndex int
	isDrained    bool
	err          error
}

func newKeyspaceIterator(manager *Manager) *Iterator {
	return &Iterator{
		manager: manager,
	}
}

// Next advances the iterator to the next item. On a new iterator, Next returns the first item.
// Returns the next keyspace (if any), and a bool value that indicates whether the next item exists (if false, it means
// the iteration is ended).
// Once the iteration is ended, all subsequent calls to Next will result in a false indicates there's no more items.
// Once an error occurs during the iteration, all subsequent calls to Next will get the same error.
func (it *Iterator) Next() (*keyspacepb.KeyspaceMeta, bool, error) {
	if it.err != nil {
		return nil, false, it.err
	}
	if it.isDrained {
		return nil, false, nil
	}

	if it.currentBatch == nil || it.currentIndex >= len(it.currentBatch) {
		if err := it.loadBatch(); err != nil {
			return nil, false, err
		}
		if it.isDrained {
			return nil, false, nil
		}
	}

	result := it.currentBatch[it.currentIndex]
	it.currentIndex++
	return result, true, nil
}

func (it *Iterator) loadBatch() error {
	nextID := uint32(0)
	if it.currentBatch != nil {
		nextID = it.currentBatch[len(it.currentBatch)-1].GetId() + 1
	}

	var err error
	it.currentIndex = 0
	batchSize := IteratorLoadingBatchSize
	failpoint.Inject("keyspaceIteratorLoadingBatchSize", func(val failpoint.Value) {
		batchSize = val.(int)
	})
	failpoint.InjectCall("keyspaceIteratorOnLoadRange")
	it.currentBatch, err = it.manager.LoadRangeKeyspace(nextID, batchSize)
	if err != nil {
		err = errors.AddStack(err)
		it.err = err
		return err
	}

	if len(it.currentBatch) == 0 {
		it.isDrained = true
	}

	return nil
}
