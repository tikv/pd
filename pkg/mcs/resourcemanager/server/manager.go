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

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/push"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/metering"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/jsonutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	persistLoopInterval       = time.Minute
	metricsCleanupInterval    = time.Minute
	metricsCleanupTimeout     = 20 * time.Minute
	defaultCollectIntervalSec = 20
	tickPerSecond             = time.Second

	pushMetricsTimeout = 10 * time.Second
)

type pushMetricsConfig struct {
	address  string
	interval time.Duration
}

func getPushMetricsConfig(controllerConfig *ControllerConfig) pushMetricsConfig {
	if controllerConfig == nil ||
		controllerConfig.PushMetricsAddress == "" ||
		controllerConfig.PushMetricsInterval.Duration <= 0 {
		return pushMetricsConfig{}
	}
	return pushMetricsConfig{
		address:  controllerConfig.PushMetricsAddress,
		interval: controllerConfig.PushMetricsInterval.Duration,
	}
}

func (cfg *pushMetricsConfig) syncPushMetricsTicker(
	newCfg pushMetricsConfig, ticker *time.Ticker,
) *time.Ticker {
	if *cfg == newCfg {
		return ticker
	}
	*cfg = newCfg
	if ticker != nil {
		ticker.Stop()
		ticker = nil
	}
	if newCfg.address == "" {
		return nil
	}
	ticker = time.NewTicker(newCfg.interval)
	return ticker
}

// Manager is the manager of resource group.
type Manager struct {
	syncutil.RWMutex
	cancel                context.CancelFunc
	wg                    sync.WaitGroup
	srv                   bs.Server
	writeRole             ResourceGroupWriteRole
	enableMetadataWatcher bool
	controllerConfig      *ControllerConfig
	krgms                 map[uint32]*keyspaceResourceGroupManager
	storage               interface {
		// Used to store the resource group settings and states.
		endpoint.ResourceGroupStorage
		// Used to get the keyspace meta info.
		endpoint.KeyspaceStorage
	}
	// consumptionChan is used to send the consumption
	// info to the background metrics flusher.
	consumptionDispatcher chan *consumptionItem
	// cached keyspace name for each keyspace ID.
	keyspaceNameLookup map[uint32]string
	// used to get the keyspace ID by name.
	keyspaceIDLookup map[string]uint32
	// metrics is the collection of metrics.
	metrics *metrics
	// ruCollector is used to collect the RU metering data.
	ruCollector *ruCollector
	// async loading state management
	loadingState atomic.Int32
	// syncLoadedGroups records groups that were loaded synchronously (e.g., by lazy loading)
	syncLoadedGroups map[trackerKey]bool
	// loadEpoch is bumped (under the manager lock) every time initMetadata
	// resets the loading state for a new term. The async loader captures it at
	// start and re-checks it before every shared-state mutation, so a loader
	// from a previous term that was blocked in a storage scan can never merge
	// stale data into, or publish completion for, a newer term.
	loadEpoch uint64
	// serviceLimitLocks serializes SetKeyspaceServiceLimit calls per keyspace
	// ID. Unlike krgm-scoped locks (e.g. defaultGroupMu), this lives on the
	// Manager itself and is never reset by Init, so it keeps serializing a
	// call parked mid-persist in an old term against a competing call in a
	// new term for the same keyspace - without it, the old call's eventual
	// publish could overwrite a newer value the new-term call already both
	// persisted and published. Entries are intentionally never removed: the
	// keyspace ID space is bounded, matching krgms/keyspaceNameLookup's own
	// grow-only lifetime, so there's no working-set pressure to justify the
	// extra bookkeeping (and its own subtleties - see LockGroup's Unlock)
	// that WithRemoveEntryOnUnlock would add.
	serviceLimitLocks *syncutil.LockGroup
}

// LoadingState represents the current loading state of resource groups
const (
	// LoadingStateNotStarted means resource groups haven't started loading
	LoadingStateNotStarted int32 = iota
	// LoadingStateInProgress means resource groups are being loaded asynchronously
	LoadingStateInProgress
	// LoadingStateCompleted means all resource groups have been loaded
	LoadingStateCompleted
)

// factoryProvider is a factory provider for the manager, which injects some specialized functions
// that need to be retrieved from the `bs.Server` instance without interacting with its interface.
type factoryProvider interface {
	GetControllerConfig() *ControllerConfig
	GetMeteringWriter() *metering.Writer
}

type metadataFactoryProvider interface {
	GetControllerConfig() *ControllerConfig
}

// writeRoleProvider is an optional provider used to configure the manager write role.
type writeRoleProvider interface {
	GetResourceGroupWriteRole() ResourceGroupWriteRole
}

// metadataWatcherProvider is an optional provider used by the independent RM service
// to switch manager bootstrap from full storage load to metadata watcher mode.
// The PD server does not implement this hook and keeps the legacy bootstrap path.
type metadataWatcherProvider interface {
	EnableResourceGroupMetadataWatcher() bool
}

func newManagerBase(controllerConfig *ControllerConfig, writeRole ResourceGroupWriteRole) *Manager {
	m := &Manager{
		writeRole:             writeRole,
		controllerConfig:      controllerConfig,
		krgms:                 make(map[uint32]*keyspaceResourceGroupManager),
		consumptionDispatcher: make(chan *consumptionItem, defaultConsumptionChanSize),
		keyspaceNameLookup:    make(map[uint32]string),
		keyspaceIDLookup:      make(map[string]uint32),
		metrics:               newMetrics(),
		ruCollector:           newRUCollector(),
		syncLoadedGroups:      make(map[trackerKey]bool),
		serviceLimitLocks:     syncutil.NewLockGroup(),
	}
	m.setLoadingState(LoadingStateNotStarted)
	return m
}

// setLoadingState publishes the loading state both to the atomic field the
// serving paths read and to the gauge operators alert on.
func (m *Manager) setLoadingState(state int32) {
	m.loadingState.Store(state)
	resourceGroupLoadingStateGauge.Set(float64(state))
}

func (m *Manager) getLoadingState() int32 {
	return m.loadingState.Load()
}

// NewManager returns a new manager base on the given server,
// which should implement the `FactoryProvider` interface.
func NewManager[T factoryProvider](srv bs.Server) *Manager {
	fp := srv.(T)
	writeRole := ResourceGroupWriteRoleLegacyAll
	if provider, ok := any(fp).(writeRoleProvider); ok {
		writeRole = provider.GetResourceGroupWriteRole()
	}
	m := newManagerBase(fp.GetControllerConfig(), writeRole)
	if provider, ok := any(fp).(metadataWatcherProvider); ok {
		m.enableMetadataWatcher = provider.EnableResourceGroupMetadataWatcher()
	}
	// The first initialization after the server is started.
	srv.AddStartCallback(func() {
		log.Info("resource group manager starts to initialize", zap.String("name", srv.Name()))
		m.storage = endpoint.NewStorageEndpoint(
			kv.NewEtcdKVBase(srv.GetClient()),
			nil,
		)
		m.srv = srv
		// Register the RU collector to the metering writer after the server is started.
		// This ensure the metering writer is started before the RU collector is registered.
		fp.GetMeteringWriter().RegisterCollector(m.ruCollector)
	})
	// The second initialization after becoming serving.
	srv.AddServiceReadyCallback(m.Init)
	return m
}

// NewMetadataOnlyManager creates a metadata-only manager without registering lifecycle callbacks.
func NewMetadataOnlyManager[T metadataFactoryProvider](srv bs.Server) (*Manager, error) {
	fp := srv.(T)
	writeRole := ResourceGroupWriteRoleLegacyAll
	if provider, ok := any(fp).(writeRoleProvider); ok {
		writeRole = provider.GetResourceGroupWriteRole()
	}
	m := newManagerBase(fp.GetControllerConfig(), writeRole)
	m.storage = endpoint.NewStorageEndpoint(
		kv.NewEtcdKVBase(srv.GetClient()),
		nil,
	)
	m.srv = srv
	if err := m.initControllerConfig(); err != nil {
		return nil, err
	}
	if err := m.loadKeyspaceResourceGroups(); err != nil {
		return nil, err
	}
	return m, nil
}

// This is used for testing only now.
func (m *Manager) close() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

// GetBasicServer returns the basic server.
func (m *Manager) GetBasicServer() bs.Server {
	return m.srv
}

// GetStorage returns the storage.
func (m *Manager) GetStorage() endpoint.ResourceGroupStorage {
	return m.storage
}

// GetWriteRole returns the manager write role.
func (m *Manager) GetWriteRole() ResourceGroupWriteRole {
	return m.writeRole
}

// GetKeyspaceServiceLimiter returns the service limit of the keyspace.
func (m *Manager) GetKeyspaceServiceLimiter(keyspaceID uint32) *serviceLimiter {
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return nil
	}
	return krgm.getServiceLimiter().Clone()
}

// SetKeyspaceServiceLimit sets the service limit of the keyspace.
func (m *Manager) SetKeyspaceServiceLimit(keyspaceID uint32, serviceLimit float64) error {
	if !m.writeRole.AllowsMetadataWrite() {
		return errMetadataWriteDisabled
	}
	// Serialize against every other SetKeyspaceServiceLimit call for this
	// keyspace, including ones that started in a different term: krgm
	// identity resets on every Init, but this lock doesn't, so it's what
	// keeps a call parked mid-persist in an old term from clobbering a
	// competing new-term call's result once it resumes. See the
	// serviceLimitLocks field doc for why a krgm-scoped lock (like
	// defaultGroupMu) can't provide this guarantee on its own.
	m.serviceLimitLocks.Lock(keyspaceID)
	defer m.serviceLimitLocks.Unlock(keyspaceID)

	// If the keyspace is not found, create a new keyspace resource group manager.
	krgm := m.getOrCreateKeyspaceResourceGroupManager(keyspaceID, true)
	failpoint.InjectCall("setServiceLimitBeforeStorage")
	// Storage phase: this persists synchronously inside setServiceLimit.
	krgm.setServiceLimit(serviceLimit)
	// Publish phase: mirror the persisted value into whichever keyspace
	// manager is current now, in case Init replaced the whole krgms map for
	// a new term while the storage write above was in flight - without
	// this, the write still lands in storage but only updates the detached
	// krgm's in-memory limiter, leaving the live serving cache (and
	// GetKeyspaceServiceLimiter) showing the stale pre-write value until
	// the next full reload. This mirrors the storage-then-publish shape of
	// publishResourceGroupMutation, but there's no reserved/confirmed
	// distinction to protect here: a service limit is a single scalar with
	// no CAS at the storage layer, so nothing short of the serviceLimitLocks
	// guard above tells this call whether cur already holds a newer value.
	// With that guard held for the whole call, no other SetKeyspaceServiceLimit
	// for this keyspace can run concurrently, so whatever's in storage now is
	// still what this call itself just wrote - safe to mirror unconditionally.
	m.Lock()
	cur := m.getOrCreateKeyspaceResourceGroupManagerLocked(keyspaceID)
	m.Unlock()
	if cur != krgm {
		cur.setServiceLimitFromStorage(serviceLimit)
	}
	return nil
}

// SetKeyspaceRUVersion sets the RU version for a specific keyspace in the controller config.
func (m *Manager) SetKeyspaceRUVersion(keyspaceID uint32, ruVersion int32) error {
	if !m.writeRole.AllowsMetadataWrite() {
		return errMetadataWriteDisabled
	}
	m.Lock()
	if m.controllerConfig.RUVersionPolicy == nil {
		// DefaultRUVersion (v1) means no RU model change.
		// There is currently no API to modify this global default; it is
		// intentionally fixed so that only per-keyspace overrides drive version bumps.
		m.controllerConfig.RUVersionPolicy = &RUVersionPolicy{Default: DefaultRUVersion}
	}
	if m.controllerConfig.RUVersionPolicy.Overrides == nil {
		m.controllerConfig.RUVersionPolicy.Overrides = make(map[uint32]RUVersion)
	}
	defaultVersion := m.controllerConfig.RUVersionPolicy.Default
	if ruVersion == defaultVersion {
		delete(m.controllerConfig.RUVersionPolicy.Overrides, keyspaceID)
	} else {
		m.controllerConfig.RUVersionPolicy.Overrides[keyspaceID] = ruVersion
	}
	// Capture the config object to save while still holding the lock, instead
	// of re-reading the m.controllerConfig field after unlocking below:
	// initControllerConfig can reassign that field wholesale (under the same
	// lock) on a leadership change, so an unlocked re-read races with it and
	// can end up saving a different config object than the one just mutated
	// above. Matches the pattern initControllerConfig itself already uses -
	// save a locally captured reference, never the field.
	controllerConfig := m.controllerConfig
	m.Unlock()
	return m.storage.SaveControllerConfig(controllerConfig)
}

// GetRUVersionPolicy returns a deep copy of the current RU version policy from the controller config.
// The returned value is safe to use after the lock is released.
func (m *Manager) GetRUVersionPolicy() *RUVersionPolicy {
	m.RLock()
	defer m.RUnlock()
	return m.controllerConfig.RUVersionPolicy.Clone()
}

// getOrCreateKeyspaceResourceGroupManager returns the keyspace resource group
// manager for keyspaceID, creating it if needed. When initDefault is true, it
// also ensures the default resource group is present. While async loading is
// still in progress this goes through loadResourceGroupIfNeeded, which tries
// a storage point load first so a customized default is never clobbered by a
// blindly synthesized one. Once loading has completed, any default missing
// from the cache truly doesn't exist anywhere, so it's synthesized directly.
func (m *Manager) getOrCreateKeyspaceResourceGroupManager(keyspaceID uint32, initDefault bool) *keyspaceResourceGroupManager {
	m.Lock()
	krgm := m.getOrCreateKeyspaceResourceGroupManagerLocked(keyspaceID)
	m.Unlock()
	if initDefault {
		// In metadata-watcher mode, LoadingStateCompleted only means the
		// initial watcher bootstrap finished, not that every subsequent PD
		// write has been observed yet - same caveat loadResourceGroupIfNeeded
		// documents at its own entry point. Without this guard, a request
		// landing right after bootstrap but before the watcher delivers a
		// just-written customized default would see nothing cached, take
		// this branch, and persist the built-in default over it. Always fall
		// through to loadResourceGroupIfNeeded in that mode, which does a
		// storage point load first instead of trusting the cache.
		if !m.enableMetadataWatcher && m.getLoadingState() == LoadingStateCompleted {
			// Async loading has already finished, so if the default group
			// isn't cached yet it truly doesn't exist anywhere; it's safe to
			// synthesize and persist it directly. This is a best-effort
			// pre-warm: any failure (stale term or a real storage error) is
			// already logged inside initDefaultResourceGroup, and a later
			// request for the default group will retry through
			// loadResourceGroupIfNeeded, which does surface such errors.
			_, _ = m.initDefaultResourceGroup(keyspaceID, krgm, func() bool {
				m.RLock()
				defer m.RUnlock()
				return m.krgms[keyspaceID] == krgm
			})
		} else if err := m.loadResourceGroupIfNeeded(keyspaceID, DefaultResourceGroupName); err != nil {
			log.Debug("failed to load default resource group", zap.Uint32("keyspace-id", keyspaceID), zap.Error(err))
		}
	}
	return krgm
}

// getOrCreateKeyspaceResourceGroupManagerLocked is the m.krgms lookup/create
// step of getOrCreateKeyspaceResourceGroupManager for a caller that already
// holds m.Lock().
func (m *Manager) getOrCreateKeyspaceResourceGroupManagerLocked(keyspaceID uint32) *keyspaceResourceGroupManager {
	krgm, ok := m.krgms[keyspaceID]
	if !ok {
		krgm = newKeyspaceResourceGroupManager(keyspaceID, m.storage, m.writeRole)
		m.krgms[keyspaceID] = krgm
	}
	return krgm
}

func (m *Manager) getKeyspaceResourceGroupManager(keyspaceID uint32) *keyspaceResourceGroupManager {
	m.RLock()
	defer m.RUnlock()
	return m.krgms[keyspaceID]
}

func (m *Manager) accessKeyspaceResourceGroupManager(keyspaceID uint32, groupName string) (*keyspaceResourceGroupManager, error) {
	var krgm *keyspaceResourceGroupManager
	if groupName == DefaultResourceGroupName {
		// For the default resource group, if the keyspace manager doesn't exist yet
		// and the group name is the default resource group name, we try to get or create it.
		krgm = m.getOrCreateKeyspaceResourceGroupManager(keyspaceID, true)
	} else {
		krgm = m.getKeyspaceResourceGroupManager(keyspaceID)
	}
	if krgm == nil {
		return nil, errs.ErrKeyspaceNotExists.FastGenByArgs(keyspaceID)
	}
	return krgm, nil
}

// Init initializes the resource group manager.
func (m *Manager) Init(ctx context.Context) error {
	if m.enableMetadataWatcher {
		if err := m.initControllerConfig(); err != nil {
			return err
		}
		// This context is derived from the leader/primary context, it will be canceled
		// from the outside loop when the leader/primary step down.
		ctx, m.cancel = context.WithCancel(ctx)
		if err := m.initializeMetadataWatcher(ctx); err != nil {
			m.cancel()
			m.wg.Wait()
			return err
		}
		// No async loader runs in this mode, so nothing will ever consume the
		// sync-loaded markers. Drop the map: otherwise every watcher event keeps
		// adding entries that are never removed, including for deleted groups.
		m.Lock()
		m.syncLoadedGroups = nil
		m.Unlock()
		m.setLoadingState(LoadingStateCompleted)
	} else {
		// This context is derived from the leader/primary context, it will be canceled
		// from the outside loop when the leader/primary step down.
		ctx, m.cancel = context.WithCancel(ctx)
		if err := m.initMetadata(ctx); err != nil {
			m.cancel()
			m.wg.Wait()
			return err
		}
	}
	m.wg.Add(1)
	// Start the background metrics flusher.
	go m.backgroundMetricsFlush(ctx)
	if m.writeRole.AllowsStateWrite() {
		m.wg.Add(1)
		go func() {
			defer logutil.LogPanic()
			m.persistLoop(ctx)
		}()
	}
	// TODO: Add a goroutine to loadKeyspaceResourceGroups periodically to avoid
	// the resource group exists gap between PD server and resource manager service
	// during redirection.
	log.Info("resource group manager finishes initialization")
	return nil
}

func (m *Manager) initControllerConfig() error {
	v, err := m.storage.LoadControllerConfig()
	if err != nil {
		log.Error("resource controller config load failed", zap.Error(err), zap.String("v", v))
		return err
	}
	// Unmarshal into a clone and publish it under the lock: on a
	// re-initialization after a leadership change, the previous term's
	// background goroutines may still be reading the current config.
	m.RLock()
	controllerConfig := cloneControllerConfig(m.controllerConfig)
	m.RUnlock()
	if err = json.Unmarshal([]byte(v), &controllerConfig); err != nil {
		log.Warn("un-marshall controller config failed, fallback to default", zap.Error(err), zap.String("v", v))
	}
	// re-save the config to make sure the config has been persisted. This
	// must run before controllerConfig is published into m.controllerConfig
	// below: once published, it's reachable (and mutable) by any concurrent
	// caller holding m.Lock() - e.g. SetKeyspaceRUVersion - which would race
	// with this unlocked marshal-and-save if it ran after instead.
	if m.writeRole.AllowsMetadataWrite() {
		if err := m.storage.SaveControllerConfig(controllerConfig); err != nil {
			return err
		}
	}
	m.Lock()
	m.controllerConfig = controllerConfig
	m.Unlock()
	return nil
}

func (m *Manager) initMetadata(ctx context.Context) error {
	if err := m.initControllerConfig(); err != nil {
		return err
	}

	m.Lock()
	m.krgms = make(map[uint32]*keyspaceResourceGroupManager)
	m.syncLoadedGroups = make(map[trackerKey]bool)
	m.loadEpoch++
	epoch := m.loadEpoch
	m.setLoadingState(LoadingStateNotStarted)
	m.Unlock()

	m.initReservedInCache()
	if err := m.loadServiceLimits(); err != nil {
		return err
	}

	m.wg.Add(1)
	go m.asyncLoadResourceGroups(ctx, epoch)
	return nil
}

func (m *Manager) loadServiceLimits() error {
	return m.storage.LoadServiceLimits(func(keyspaceID uint32, _ float64) {
		failpoint.InjectCall("loadServiceLimitsBeforeApply", keyspaceID)
		// Serialize against SetKeyspaceServiceLimit for this keyspace, and
		// re-read the value from storage instead of trusting the one the
		// bulk scan above already fetched: that value was read before this
		// lock was acquired, so a concurrent SetKeyspaceServiceLimit call
		// (from either this term or a still-in-flight previous one) can
		// persist and mirror a newer value in between, which this callback
		// would otherwise silently clobber with its now-stale snapshot. A
		// fresh point read taken under the same lock SetKeyspaceServiceLimit
		// itself uses is guaranteed to observe whichever write actually
		// landed last, since setServiceLimit's storage write always
		// completes before SetKeyspaceServiceLimit releases this lock.
		m.serviceLimitLocks.Lock(keyspaceID)
		defer m.serviceLimitLocks.Unlock(keyspaceID)
		serviceLimit, err := m.storage.LoadServiceLimit(keyspaceID)
		if err != nil {
			log.Warn("failed to reload service limit", zap.Uint32("keyspace-id", keyspaceID), zap.Error(err))
			return
		}
		m.getOrCreateKeyspaceResourceGroupManager(keyspaceID, false).setServiceLimitFromStorage(serviceLimit)
	})
}

func (m *Manager) loadKeyspaceResourceGroups() error {
	tempKrgms, err := m.loadKeyspaceResourceGroupsFromStorage()
	if err != nil {
		return err
	}
	m.Lock()
	m.krgms = tempKrgms
	m.syncLoadedGroups = nil
	m.setLoadingState(LoadingStateCompleted)
	epoch := m.loadEpoch
	m.Unlock()
	// This runs to completion before the manager is exposed to any request
	// (NewMetadataOnlyManager's caller has not returned yet), so there is no
	// live writer to race against; eagerly confirming every loaded keyspace's
	// default here is safe, unlike the same backfill from the async loader.
	m.initReserved(epoch)
	return m.loadServiceLimits()
}

// storeLoadingStateIfCurrent stores the loading state only if the manager has
// not been reinitialized since the loader with the given epoch started. It
// returns false when the loader is stale and must exit without touching any
// further shared state.
func (m *Manager) storeLoadingStateIfCurrent(epoch uint64, state int32) bool {
	m.Lock()
	defer m.Unlock()
	if m.loadEpoch != epoch {
		return false
	}
	m.setLoadingState(state)
	return true
}

func (m *Manager) asyncLoadResourceGroups(ctx context.Context, epoch uint64) {
	defer logutil.LogPanic()
	defer m.wg.Done()

	const retryInterval = 10 * time.Second
	retry := 0
	for {
		select {
		case <-ctx.Done():
			log.Info("async loading resource groups cancelled")
			return
		default:
		}
		if retry > 0 {
			log.Info("retrying async loading resource groups", zap.Int("retry", retry))
			timer := time.NewTimer(retryInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				log.Info("async loading resource groups cancelled")
				return
			case <-timer.C:
			}
		}

		if !m.storeLoadingStateIfCurrent(epoch, LoadingStateInProgress) {
			log.Info("async loading resource groups aborted: manager was reinitialized")
			return
		}
		startTime := time.Now()
		tempKrgms, err := m.loadKeyspaceResourceGroupsFromStorage()
		// The storage scans above can block for a long time; re-check for
		// cancellation before touching any shared state, so a loader whose
		// term already ended doesn't pollute a newer term's state.
		select {
		case <-ctx.Done():
			log.Info("async loading resource groups cancelled")
			return
		default:
		}
		if err != nil {
			// Use warn level since the loader retries indefinitely until it succeeds.
			// The failure counter and the loading state gauge are what make a load
			// that never succeeds alertable, since it no longer fails `Init` loudly.
			asyncLoadGroupFailureCounter.Inc()
			log.Warn("failed to load resource groups", zap.Error(err), zap.Int("retry", retry))
			if !m.storeLoadingStateIfCurrent(epoch, LoadingStateNotStarted) {
				log.Info("async loading resource groups aborted: manager was reinitialized")
				return
			}
			retry++
			continue
		}

		// Flatten the loaded groups so the merge below can run in bounded
		// batches. Holding m.Lock across the whole O(total groups) merge would
		// block every concurrent point/token request (they need the lock to
		// resolve a keyspace manager) for the entire merge, causing a large
		// latency spike right when async loading completes on a cluster with
		// many resource groups.
		type mergeItem struct {
			keyspaceID uint32
			name       string
			group      *ResourceGroup
		}
		// Size the slice up front: it holds every loaded group, which is exactly
		// the scale this loader exists to handle. tempKrgms is a local map this
		// goroutine alone constructed and holds - not yet reachable from
		// m.krgms or any other goroutine - so reading it here needs no locking;
		// only the individual *ResourceGroup values get published later, into
		// whichever keyspace manager is current at merge time, not tempKrgm
		// itself.
		totalGroups := 0
		for _, tempKrgm := range tempKrgms {
			totalGroups += len(tempKrgm.groups)
		}
		pending := make([]mergeItem, 0, totalGroups)
		for keyspaceID, tempKrgm := range tempKrgms {
			for name, group := range tempKrgm.groups {
				pending = append(pending, mergeItem{keyspaceID: keyspaceID, name: name, group: group})
			}
		}

		const mergeBatchSize = 1024
		loaded := 0
		aborted := false
		for start := 0; start < len(pending); start += mergeBatchSize {
			end := min(start+mergeBatchSize, len(pending))
			m.Lock()
			if m.loadEpoch != epoch {
				// The manager was reinitialized for a new term while this
				// loader was scanning; its result is stale and must not be
				// merged. Re-checked every batch since a term change can land
				// between batches.
				m.Unlock()
				aborted = true
				break
			}
			for _, it := range pending[start:end] {
				key := trackerKey{keyspaceID: it.keyspaceID, groupName: it.name}
				if m.syncLoadedGroups[key] {
					continue
				}
				krgm := m.getOrCreateKeyspaceResourceGroupManagerLocked(it.keyspaceID)
				krgm.Lock()
				krgm.groups[it.name] = it.group
				// This group is now confirmed, fully-loaded data (settings
				// and state); it must no longer be treated as an unconfirmed
				// placeholder by loadResourceGroupIfNeeded or skipped by the
				// state persist loop.
				delete(krgm.reservedGroups, it.name)
				failpoint.InjectCall("mergeBeforeBurstSync", it.keyspaceID, it.name)
				// Sync burstability while still holding krgm's lock, so the
				// group never becomes visible to a concurrent reader with an
				// unsynced burst setting - see syncBurstabilityWithServiceLimitLocked.
				krgm.syncBurstabilityWithServiceLimitLocked(it.group)
				krgm.Unlock()
				loaded++
			}
			m.Unlock()
		}
		if aborted {
			log.Info("async loading resource groups aborted: manager was reinitialized")
			return
		}
		m.Lock()
		if m.loadEpoch != epoch {
			m.Unlock()
			log.Info("async loading resource groups aborted: manager was reinitialized")
			return
		}
		m.syncLoadedGroups = nil
		m.Unlock()

		// No eager reserved-default backfill runs here. An eager pass would
		// re-resolve every keyspace manager and persist a synthetic default
		// concurrently with live requests: once completion is published below,
		// a request for a keyspace whose default was never customized can
		// synthesize and publish it (via getOrCreateKeyspaceResourceGroupManager
		// or loadResourceGroupIfNeeded's confirmed-not-found path) in the same
		// goroutine as its own subsequent write, with no independent writer
		// racing it. An out-of-band backfill loop has no such ordering guarantee
		// against those on-demand writers, so it can persist a synthetic default
		// after a concurrent customized write and silently discard it. A
		// keyspace nobody ever queries needs no persisted default: the persist
		// loop already skips unconfirmed reserved placeholders, so leaving one
		// reserved indefinitely is a normal, accepted state, not a leak.
		if !m.storeLoadingStateIfCurrent(epoch, LoadingStateCompleted) {
			log.Info("async loading resource groups aborted: manager was reinitialized")
			return
		}
		duration := time.Since(startTime)
		asyncLoadGroupDuration.Observe(duration.Seconds())
		log.Info("async loading resource groups completed", zap.Int("loaded-groups", loaded), zap.Duration("duration", duration))
		return
	}
}

func (m *Manager) loadKeyspaceResourceGroupsFromStorage() (map[uint32]*keyspaceResourceGroupManager, error) {
	tempKrgms := make(map[uint32]*keyspaceResourceGroupManager)
	getOrCreateTempKrgm := func(keyspaceID uint32) *keyspaceResourceGroupManager {
		krgm, ok := tempKrgms[keyspaceID]
		if !ok {
			krgm = newKeyspaceResourceGroupManager(keyspaceID, m.storage, m.writeRole)
			tempKrgms[keyspaceID] = krgm
		}
		return krgm
	}
	if err := m.storage.LoadResourceGroupSettings(func(keyspaceID uint32, name string, rawValue string) {
		err := getOrCreateTempKrgm(keyspaceID).addResourceGroupFromRaw(name, rawValue)
		if err != nil {
			log.Error("failed to add resource group to the keyspace resource group manager",
				zap.Uint32("keyspace-id", keyspaceID), zap.String("group-name", name), zap.Error(err))
		}
	}); err != nil {
		return nil, err
	}
	if err := m.storage.LoadResourceGroupStates(func(keyspaceID uint32, name string, rawValue string) {
		krgm := tempKrgms[keyspaceID]
		if krgm == nil {
			log.Warn("failed to get the corresponding keyspace resource group manager",
				zap.Uint32("keyspace-id", keyspaceID), zap.String("group-name", name))
			return
		}
		err := krgm.setRawStatesIntoResourceGroup(name, rawValue)
		if err != nil {
			log.Error("failed to set resource group state",
				zap.Uint32("keyspace-id", keyspaceID), zap.String("group-name", name), zap.Error(err))
		}
	}); err != nil {
		return nil, err
	}
	return tempKrgms, nil
}

// loadResourceGroup loads a single resource group from storage.
func (m *Manager) loadResourceGroup(keyspaceID uint32, name string) (*ResourceGroup, error) {
	rawValue, err := m.storage.LoadResourceGroupSetting(keyspaceID, name)
	if err != nil {
		return nil, err
	}
	if rawValue == "" {
		return nil, errs.ErrResourceGroupNotExists.FastGenByArgs(name)
	}
	krgm := newKeyspaceResourceGroupManager(keyspaceID, m.storage, m.writeRole)
	if err := krgm.addResourceGroupFromRaw(name, rawValue); err != nil {
		return nil, err
	}
	state, err := m.storage.LoadResourceGroupState(keyspaceID, name)
	if err != nil {
		log.Warn("failed to load resource group state",
			zap.Uint32("keyspace-id", keyspaceID),
			zap.String("group-name", name),
			zap.Error(err))
		return nil, err
	}
	if state != "" {
		if err := krgm.setRawStatesIntoResourceGroup(name, state); err != nil {
			return nil, err
		}
	}
	return krgm.getMutableResourceGroup(name), nil
}

func (m *Manager) loadResourceGroupIfNeeded(keyspaceID uint32, name string) error {
	// In metadata-watcher mode the cache is only eventually consistent with
	// storage: PD writes metadata directly and the watcher applies it
	// asynchronously, so LoadingStateCompleted here only means the initial
	// watcher bootstrap finished, not that every subsequent write has been
	// observed yet. Always fall through to a point load in that mode so a
	// write that outraces its own watcher event is still visible.
	if !m.enableMetadataWatcher && m.getLoadingState() == LoadingStateCompleted {
		return nil
	}
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm != nil {
		// A cached entry only satisfies this call if it's confirmed data, not
		// just a synthetic placeholder (e.g. from ensureReservedDefaultGroupInCache)
		// installed before async loading had a chance to run.
		if krgm.hasConfirmedResourceGroup(name) {
			return nil
		}
	}
	// The lock-free storage read below can be invalidated while it runs: a
	// concurrent Delete of any group in the keyspace bumps deleteGen, and a
	// leadership change replaces m.krgms and m.syncLoadedGroups (bumping
	// loadEpoch). Both are rare: retry the read a few times against freshly
	// captured state so neither makes this request spuriously fail or publish
	// into a detached manager; if it keeps racing, give up without inserting
	// and let a later request or the async bulk merge reload the group.
	const maxLoadAttempts = 3
	for attempt := 1; ; attempt++ {
		// Capture the load epoch and the current keyspace manager atomically,
		// and re-capture them on every retry: publishing into a previous
		// term's detached manager while marking the new term's map would make
		// the new bulk merge skip a group its cache doesn't contain.
		m.Lock()
		epoch := m.loadEpoch
		krgm = m.getOrCreateKeyspaceResourceGroupManagerLocked(keyspaceID)
		m.Unlock()
		// Snapshot the delete generation before the lock-free storage read,
		// so a Delete that lands after the read is detected under the insert
		// lock and can't be undone by the now-stale result.
		deleteGen := krgm.loadDeleteGen()
		group, err := m.loadResourceGroup(keyspaceID, name)
		if err != nil {
			if name == DefaultResourceGroupName && errs.ErrResourceGroupNotExists.Equal(err) {
				m.RLock()
				stale := m.loadEpoch != epoch || m.krgms[keyspaceID] != krgm
				m.RUnlock()
				if stale {
					if attempt >= maxLoadAttempts {
						// Exhausted retries without confirming absence; report
						// a retryable loading error rather than a bogus
						// success the caller would mistake for a load.
						return errs.ErrResourceGroupsLoading
					}
					continue
				}
				// No persisted default group settings exist yet (e.g. a brand-new
				// keyspace), so it's safe to synthesize the reserved default group.
				// This calls initDefaultResourceGroup directly instead of going
				// through getOrCreateKeyspaceResourceGroupManager(id, true), which
				// now routes back into this same function and would recurse.
				stillCurrent := func() bool {
					m.RLock()
					defer m.RUnlock()
					return m.loadEpoch == epoch && m.krgms[keyspaceID] == krgm
				}
				// initDefaultResourceGroup publishes through
				// publishResourceGroupMutation, which sets the sync-loaded
				// marker itself atomically with the cache effect - no
				// separate marker-setting step is needed here.
				_, initErr := m.initDefaultResourceGroup(keyspaceID, krgm, stillCurrent)
				if initErr != nil {
					if errs.ErrResourceGroupsLoading.Equal(initErr) {
						// stillCurrent caught a term change that landed after the
						// stale check above but before the persist started; the
						// group was not created anywhere, so retry against the
						// fresh term instead of reporting a bogus success.
						if attempt >= maxLoadAttempts {
							return errs.ErrResourceGroupsLoading
						}
						continue
					}
					// A real failure persisting the default group (e.g. a storage
					// write error): propagate it rather than silently reporting
					// success for a group that was never actually created.
					return initErr
				}
				return nil
			}
			return err
		}
		inserted := false
		m.Lock()
		if m.loadEpoch != epoch || m.krgms[keyspaceID] != krgm {
			// The manager was reinitialized for a new term while the storage
			// read was in flight; retry against the new term's state.
			m.Unlock()
			if attempt >= maxLoadAttempts {
				// Exhausted retries without publishing the group; report a
				// retryable loading error rather than a bogus success that
				// would surface an existing group as nonexistent.
				return errs.ErrResourceGroupsLoading
			}
			continue
		}
		krgm.Lock()
		if krgm.deleteGen != deleteGen {
			krgm.Unlock()
			m.Unlock()
			if attempt >= maxLoadAttempts {
				// Exhausted retries without publishing the group; report a
				// retryable loading error rather than a bogus success that
				// would surface an existing group as nonexistent.
				return errs.ErrResourceGroupsLoading
			}
			continue
		}
		if _, exists := krgm.groups[name]; !exists {
			krgm.groups[name] = group
			inserted = true
		} else if _, reserved := krgm.reservedGroups[name]; reserved {
			// The existing entry is just an unconfirmed placeholder; the freshly
			// loaded group is the real, confirmed data, so replacing it is safe.
			krgm.groups[name] = group
			inserted = true
		}
		delete(krgm.reservedGroups, name)
		if inserted {
			// Sync burstability while krgm's lock is still held, so the group
			// never becomes visible to a concurrent reader with an unsynced
			// burst setting - see syncBurstabilityWithServiceLimitLocked.
			krgm.syncBurstabilityWithServiceLimitLocked(group)
		}
		krgm.Unlock()
		m.markResourceGroupSyncLoadedLocked(keyspaceID, name)
		m.Unlock()
		failpoint.Inject("lazyLoadAfterCachePublish", func() {})
		syncLoadGroupCounter.Inc()
		return nil
	}
}

// markResourceGroupSyncLoaded records that the group in krgm was written or
// fully loaded synchronously. The caller passes the keyspace manager it
// actually mutated: if that manager is no longer the live one (the manager was
// reinitialized for a new term while the caller was blocked on storage I/O),
// the marker is skipped, since marking the new term's map for a group its
// cache doesn't contain would make the bulk merge skip loading it.
func (m *Manager) markResourceGroupSyncLoaded(keyspaceID uint32, krgm *keyspaceResourceGroupManager, name string) {
	m.Lock()
	defer m.Unlock()
	if m.krgms[keyspaceID] != krgm {
		return
	}
	m.markResourceGroupSyncLoadedLocked(keyspaceID, name)
}

// markResourceGroupSyncLoadedLocked is markResourceGroupSyncLoaded's marker
// write for a caller that already holds m.Lock() and has already verified
// krgm is still the live entry for keyspaceID.
func (m *Manager) markResourceGroupSyncLoadedLocked(keyspaceID uint32, name string) {
	if m.syncLoadedGroups != nil {
		m.syncLoadedGroups[trackerKey{keyspaceID: keyspaceID, groupName: name}] = true
	}
}

// publishResourceGroupMutation applies a metadata mutation's cache effect and
// its sync-loaded marker atomically with respect to the async bulk merge,
// against whichever keyspace manager is current at publish time. The merge
// holds the manager lock across its whole merge step, so the two can only be
// fully ordered: publish first and the merge skips the marked group; merge
// first and the publish overrides its stale snapshot.
//
// krgm is the keyspace manager the caller actually persisted the mutation's
// storage phase against. If it's no longer the live manager for keyspaceID
// (Init replaced the whole krgms map for a new term while the caller was
// blocked on storage I/O) AND the new term already has confirmed data for
// this group - installed by its own bulk merge, lazy load, or a live
// Add/Modify/Delete that raced ahead of this delayed one - fn's result was
// computed from data read in the old term and is stale relative to that
// confirmed data; applying it here would silently overwrite it in the cache
// and, via the sync-loaded marker, hide the group from the new term's bulk
// merge too, so the mutation is dropped instead. If the new term hasn't
// confirmed this group yet (missing, or still a reserved placeholder), there
// is nothing newer to protect, so fn's result is applied into the new term's
// manager - the two async-load tests exercise exactly this case (a mutation
// straddling a leadership change with no competing new-term write) and
// require it to still take effect.
//
// fn runs with the keyspace manager write lock held and must not do I/O; it
// returns whether to record the sync-loaded marker and, when a group was
// (re)installed, the group to sync burstability for.
//
// Known gap: this "skip if the new term already confirmed the group" rule
// assumes the confirming write's storage effect is genuinely the latest one.
// That assumption can fail for any mutation kind, not just Delete:
//
//   - Delete can be parked before its storage phase (deleteResourceGroupBeforeStorage).
//     If an old-term Delete is parked there, a new-term Add for the same group
//     persists and publishes (getting confirmed), and only then does the old
//     Delete's storage removal finally run, it deletes that newer write in
//     storage - making Delete genuinely the last writer - but this function
//     skips its publish because the group is already "confirmed", leaving the
//     cache showing the deleted group as present.
//   - Add/Modify's own storage write always completes before either of them can
//     be parked (see addResourceGroupBeforePublish/modifyResourceGroupBeforePublish,
//     both placed after the storage write) - but that only rules out being
//     parked *after* persisting, not the storage write itself taking real
//     wall-clock time to land. A new-term bulk merge or lazy load can read and
//     confirm an older snapshot while an old-term Add/Modify's storage write is
//     still in flight; when that write then completes, it's the latest value in
//     storage, but this function still sees "already confirmed" for the older
//     snapshot and drops the publish, leaving the cache stale relative to
//     storage indefinitely.
//
// In both cases nothing re-syncs the cache afterward, since it reads as
// confirmed to every other path too. The Delete case also mirrors, in the
// opposite direction, a gap the old unconditional-apply code had (a delayed
// Delete publish could instead wipe a newer confirmed Add). None of this is
// fully closed without a storage-side revision to tell which write actually
// landed last; that's the same class of gap tracked for
// initDefaultResourceGroup's stillCurrent check. No regression test exercises
// either interleaving yet.
//
// TODO(#11105): close this gap with a storage-side revision/CAS check.
func (m *Manager) publishResourceGroupMutation(
	keyspaceID uint32, name string, krgm *keyspaceResourceGroupManager,
	fn func(krgm *keyspaceResourceGroupManager) (mark bool, synced *ResourceGroup),
) {
	m.Lock()
	defer m.Unlock()
	cur := m.getOrCreateKeyspaceResourceGroupManagerLocked(keyspaceID)
	if cur != krgm && cur.hasConfirmedResourceGroup(name) {
		log.Info("skip publishing resource group mutation: a newer confirmed write already exists",
			zap.Uint32("keyspace-id", keyspaceID), zap.String("group-name", name))
		return
	}
	cur.Lock()
	mark, synced := fn(cur)
	if synced != nil {
		// Sync burstability while cur's lock is still held, so the group
		// never becomes visible to a concurrent reader with an unsynced
		// burst setting - see syncBurstabilityWithServiceLimitLocked.
		cur.syncBurstabilityWithServiceLimitLocked(synced)
	}
	cur.Unlock()
	failpoint.InjectCall("publishMutationBeforeMark")
	if mark {
		m.markResourceGroupSyncLoadedLocked(keyspaceID, name)
	}
}

// initDefaultResourceGroup synthesizes and persists the built-in default
// group for keyspaceID into krgm if nothing confirmed exists yet, publishing
// it through publishResourceGroupMutation - the same path a real
// Add/ModifyResourceGroup uses - so the cache effect and the sync-loaded
// marker land atomically with respect to the async bulk merge. This used to
// publish through krgm's own lock only, with the caller setting the marker
// afterward in a separate critical section; that left a window where the
// bulk merge could run in between and replace the synthesized group - along
// with any live consumption/token update applied to it in that window - with
// its own possibly-stale scanned copy, since it had no way yet to know the
// group was confirmed. Routing through publishResourceGroupMutation closes
// that window the same way it already does for Add/Modify/Delete.
//
// created reports whether it actually performed a synthesis. The three
// (created, err) outcomes need different handling and must not be collapsed
// into a single bool by the caller: (false, nil) means confirmed data
// already existed - nothing to do, safe to treat as success; (false,
// errs.ErrResourceGroupsLoading) means stillCurrent caught a term change
// before the persist started - the group was not created anywhere, so the
// caller must retry against the fresh term rather than treat this as
// success; (false, any other non-nil error) means the persist itself failed
// (e.g. a storage write error) - the caller must propagate it rather than
// silently swallow a real failure as success.
//
// defaultGroupMu only serializes callers that share the krgm instance; it
// does nothing across a term change, since Init gives the new term an
// entirely new krgm object with its own, separate defaultGroupMu.
// stillCurrent, when non-nil, is checked immediately before the persist
// (right after defaultGroupMu is acquired) to fail fast and avoid a wasted
// storage write when the caller already knows krgm is stale - typically by
// comparing it against the manager's live entry for its keyspace ID. It is
// no longer the only guard against a term change landing while the persist's
// storage write is in flight: publishResourceGroupMutation's own
// cur != krgm && cur.hasConfirmedResourceGroup(name) check now also protects
// that window (and, if the new term hasn't confirmed a default yet, still
// applies this call's result into the new term's live krgm instead of
// silently dropping it into a detached one). Pass nil when no such
// fail-fast check is needed or available (e.g. in tests that exercise a
// krgm/Manager pair with no concurrent writer to race against).
func (m *Manager) initDefaultResourceGroup(keyspaceID uint32, krgm *keyspaceResourceGroupManager, stillCurrent func() bool) (created bool, err error) {
	// A confirmed cached entry means initialization is unnecessary; a missing
	// or reserved-placeholder entry means nothing is persisted for the
	// default group (e.g. a fresh store), so it must still be created and
	// persisted, otherwise its settings are never stored and state
	// persistence stays skipped.
	if krgm.hasConfirmedResourceGroup(DefaultResourceGroupName) {
		return false, nil
	}
	// Serialize against every other synthesis or real Add/ModifyResourceGroup
	// targeting "default" that shares this krgm instance: see the
	// defaultGroupMu doc comment on the struct.
	krgm.defaultGroupMu.Lock()
	defer krgm.defaultGroupMu.Unlock()
	// Re-check under defaultGroupMu: while this goroutine waited for the
	// lock, a real write may have already confirmed the default group, in
	// which case synthesizing here would silently clobber it.
	if krgm.hasConfirmedResourceGroup(DefaultResourceGroupName) {
		return false, nil
	}
	if stillCurrent != nil && !stillCurrent() {
		return false, errs.ErrResourceGroupsLoading
	}
	defaultGroup := newDefaultResourceGroup()
	group, err := krgm.persistResourceGroup(defaultGroup.IntoProtoResourceGroup(krgm.keyspaceID))
	if err != nil {
		log.Warn("init default group failed", zap.Uint32("keyspace-id", krgm.keyspaceID), zap.Error(err))
		return false, err
	}
	m.publishResourceGroupMutation(keyspaceID, DefaultResourceGroupName, krgm, func(cur *keyspaceResourceGroupManager) (bool, *ResourceGroup) {
		cur.groups[group.Name] = group
		delete(cur.reservedGroups, group.Name)
		return true, group
	})
	return true, nil
}

func (m *Manager) isResourceGroupLoadingComplete() bool {
	return m.getLoadingState() == LoadingStateCompleted
}

func cloneControllerConfig(cfg *ControllerConfig) *ControllerConfig {
	if cfg == nil {
		return nil
	}
	cloned := *cfg
	cloned.RUVersionPolicy = cfg.RUVersionPolicy.Clone()
	return &cloned
}

func (m *Manager) applyControllerConfigFromRaw(rawValue string) error {
	controllerConfig := &ControllerConfig{}
	if err := json.Unmarshal([]byte(rawValue), controllerConfig); err != nil {
		log.Error("failed to apply controller config from watcher",
			zap.String("raw-value", rawValue),
			zap.Error(err))
		return err
	}
	m.Lock()
	m.controllerConfig = controllerConfig
	m.Unlock()
	return nil
}

func (m *Manager) applyResourceGroupSettingFromRaw(keyspaceID uint32, name, rawValue string) error {
	krgm := m.getOrCreateKeyspaceResourceGroupManager(keyspaceID, false)
	krgm.ensureReservedDefaultGroupInCache()
	if err := krgm.upsertResourceGroupFromRaw(name, rawValue); err != nil {
		log.Error("failed to apply resource group settings from watcher",
			zap.Uint32("keyspace-id", keyspaceID),
			zap.String("group-name", name),
			zap.String("raw-value", rawValue),
			zap.Error(err))
		return err
	}
	m.markResourceGroupSyncLoaded(keyspaceID, krgm, name)
	return nil
}

func (m *Manager) applyServiceLimitFromRaw(keyspaceID uint32, rawValue string) error {
	var serviceLimit float64
	if err := json.Unmarshal([]byte(rawValue), &serviceLimit); err != nil {
		log.Error("failed to apply service limit from watcher",
			zap.Uint32("keyspace-id", keyspaceID),
			zap.String("raw-value", rawValue),
			zap.Error(err))
		return err
	}
	krgm := m.getOrCreateKeyspaceResourceGroupManager(keyspaceID, false)
	krgm.ensureReservedDefaultGroupInCache()
	krgm.setServiceLimitFromStorage(serviceLimit)
	return nil
}

func (m *Manager) applyResourceGroupStatesFromRaw(keyspaceID uint32, name, rawValue string) error {
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		// LoopWatcher bootstrap loads keys in lexicographic order, so settings are loaded
		// before states. If a live watch delivers states before the corresponding settings
		// create the manager, we drop the update and rely on the next persisted states sync.
		log.Debug("skip applying resource group states without corresponding manager",
			zap.Uint32("keyspace-id", keyspaceID), zap.String("group-name", name))
		return nil
	}
	if err := krgm.setRawStatesIntoResourceGroup(name, rawValue); err != nil {
		log.Error("failed to apply resource group states from watcher",
			zap.Uint32("keyspace-id", keyspaceID),
			zap.String("group-name", name),
			zap.String("raw-value", rawValue),
			zap.Error(err))
		return err
	}
	m.markResourceGroupSyncLoaded(keyspaceID, krgm, name)
	return nil
}

// initReserved backfills the default resource group for every keyspace whose
// default wasn't confirmed by loading. The caller must have just verified
// epoch via storeLoadingStateIfCurrent; re-verify it here under the manager
// lock immediately before touching krgms, since that earlier check alone
// does not cover this call once its lock is released.
//
// Only call this from a path that runs before the manager serves any
// request (construction-time full loads), not from the async loader: once
// requests are flowing, a concurrent Add/ModifyResourceGroup can confirm a
// keyspace's default between this function's existence check and its
// unconditional persist, and this backfill's synthetic write would then
// silently clobber that write in both storage and the live cache. Live
// requests already synthesize a missing default on demand (via
// getOrCreateKeyspaceResourceGroupManager or loadResourceGroupIfNeeded's
// confirmed-not-found path) sequenced with their own subsequent write, so no
// out-of-band backfill is needed once serving has started.
func (m *Manager) initReserved(epoch uint64) {
	m.Lock()
	if m.loadEpoch != epoch {
		m.Unlock()
		log.Info("skip initReserved: manager was reinitialized")
		return
	}
	m.Unlock()
	// Initialize the null keyspace resource group manager if it doesn't exist.
	m.getOrCreateKeyspaceResourceGroupManager(constant.NullKeyspaceID, true)
	// Initialize the default resource group respectively for each keyspace if it doesn't exist.
	// No stillCurrent check is needed: this whole function only runs before
	// the manager serves any request (see the doc comment above), so there is
	// no concurrent Add/ModifyResourceGroup or other initDefaultResourceGroup
	// call to race against.
	for _, krgm := range m.getKeyspaceResourceGroupManagers() {
		// Any failure is already logged inside initDefaultResourceGroup; a
		// later request for the default group retries through
		// loadResourceGroupIfNeeded once serving starts.
		_, _ = m.initDefaultResourceGroup(krgm.keyspaceID, krgm, nil)
	}
}

func (m *Manager) initReservedInCache() {
	// Initialize the reserved default group in memory before async loading
	// without overwriting persisted default group settings.
	m.getOrCreateKeyspaceResourceGroupManager(constant.NullKeyspaceID, false).ensureReservedDefaultGroupInCache()
	for _, krgm := range m.getKeyspaceResourceGroupManagers() {
		krgm.ensureReservedDefaultGroupInCache()
	}
}

// UpdateControllerConfigItem updates the controller config item.
func (m *Manager) UpdateControllerConfigItem(key string, value any) error {
	if !m.writeRole.AllowsMetadataWrite() {
		return errMetadataWriteDisabled
	}
	kp := strings.Split(key, ".")
	if len(kp) == 0 {
		return errors.Errorf("invalid key %s", key)
	}
	m.Lock()
	controllerConfig := cloneControllerConfig(m.controllerConfig)
	var config any
	switch kp[0] {
	case "request-unit":
		config = &controllerConfig.RequestUnit
	default:
		config = controllerConfig
	}
	updated, found, err := jsonutil.AddKeyValue(config, kp[len(kp)-1], value)
	if err != nil {
		m.Unlock()
		return err
	}

	if !found {
		m.Unlock()
		return errors.Errorf("config item %s not found", key)
	}
	// Validate RUVersionPolicy after any update, regardless of the key path,
	// since the default branch merges into the full ControllerConfig.
	if err := controllerConfig.RUVersionPolicy.validate(); err != nil {
		m.Unlock()
		return err
	}
	if updated {
		if err := m.storage.SaveControllerConfig(controllerConfig); err != nil {
			m.Unlock()
			log.Error("save controller config failed", zap.Error(err))
			return err
		}
		m.controllerConfig = controllerConfig
	}
	m.Unlock()
	if updated {
		log.Info("updated controller config item", zap.String("key", key), zap.Any("value", value))
	}
	return nil
}

// GetControllerConfig returns the controller config.
func (m *Manager) GetControllerConfig() *ControllerConfig {
	m.RLock()
	defer m.RUnlock()
	return cloneControllerConfig(m.controllerConfig)
}

// AddResourceGroup puts a resource group.
// NOTE: AddResourceGroup should also be idempotent because tidb depends
// on this retry mechanism.
func (m *Manager) AddResourceGroup(grouppb *rmpb.ResourceGroup) error {
	if !m.writeRole.AllowsMetadataWrite() {
		return errMetadataWriteDisabled
	}
	keyspaceID := ExtractKeyspaceID(grouppb.GetKeyspaceId())
	// If the keyspace is not initialized, it means this is the first resource group created for this keyspace,
	// so we need to initialize the default resource group for the keyspace as well.
	krgm := m.getOrCreateKeyspaceResourceGroupManager(keyspaceID, true)
	if krgm == nil {
		return errs.ErrKeyspaceNotExists.FastGenByArgs(keyspaceID)
	}
	if err := m.loadResourceGroupIfNeeded(keyspaceID, grouppb.Name); err != nil &&
		!errs.ErrResourceGroupNotExists.Equal(err) {
		log.Warn("failed to load resource group before add", zap.Uint32("keyspace-id", keyspaceID), zap.String("name", grouppb.Name), zap.Error(err))
		return err
	}
	if grouppb.Name == DefaultResourceGroupName {
		// Serialize against a concurrent on-demand synthesis of the same
		// default group (initDefaultResourceGroup, e.g. from another
		// request's getOrCreateKeyspaceResourceGroupManager/
		// loadResourceGroupIfNeeded call): without this, the synthetic
		// write's storage/cache commit can land after this real write's,
		// silently discarding these customized settings.
		krgm.defaultGroupMu.Lock()
		defer krgm.defaultGroupMu.Unlock()
	}
	// Storage phase: validate and persist. Publishing the cache effect is done
	// separately below, against krgm if it's still the live manager for
	// keyspaceID by then, or dropped otherwise - see publishResourceGroupMutation.
	group, err := krgm.persistResourceGroup(grouppb)
	if err != nil {
		return err
	}
	failpoint.InjectCall("addResourceGroupBeforePublish")
	m.publishResourceGroupMutation(keyspaceID, grouppb.Name, krgm, func(cur *keyspaceResourceGroupManager) (bool, *ResourceGroup) {
		cur.groups[group.Name] = group
		delete(cur.reservedGroups, group.Name)
		return true, group
	})
	return nil
}

// ModifyResourceGroup modifies an existing resource group.
func (m *Manager) ModifyResourceGroup(grouppb *rmpb.ResourceGroup) error {
	if !m.writeRole.AllowsMetadataWrite() {
		return errMetadataWriteDisabled
	}
	keyspaceID := ExtractKeyspaceID(grouppb.GetKeyspaceId())
	if err := m.loadResourceGroupIfNeeded(keyspaceID, grouppb.Name); err != nil {
		log.Debug("failed to load resource group", zap.Uint32("keyspace-id", keyspaceID), zap.String("name", grouppb.Name), zap.Error(err))
		return err
	}
	krgm, err := m.accessKeyspaceResourceGroupManager(keyspaceID, grouppb.Name)
	if err != nil {
		return err
	}
	if grouppb.Name == DefaultResourceGroupName {
		// Serialize against a concurrent on-demand synthesis of the same
		// default group; see the matching guard in AddResourceGroup.
		krgm.defaultGroupMu.Lock()
		defer krgm.defaultGroupMu.Unlock()
	}
	patched, err := krgm.modifyResourceGroup(grouppb)
	if err != nil {
		return err
	}
	failpoint.InjectCall("modifyResourceGroupBeforePublish")
	m.publishResourceGroupMutation(keyspaceID, grouppb.Name, krgm, func(cur *keyspaceResourceGroupManager) (bool, *ResourceGroup) {
		var synced *ResourceGroup
		if existing := cur.groups[grouppb.Name]; existing != patched {
			// A different object sits in the current term's cache: either the
			// group is missing, a reserved default placeholder (with synthetic
			// token/consumption state), or a pre-modification bulk-merge
			// snapshot. Install `patched` wholesale rather than only patching
			// settings onto it. `patched` was loaded/confirmed before it was
			// modified, so it carries both the modified settings and the
			// group's confirmed running state - patching the placeholder in
			// place would keep its synthetic state, which the marker below
			// would then freeze as confirmed and the persist loop would write
			// back over the real state.
			cur.groups[grouppb.Name] = patched
			synced = patched
		}
		// The settings and state are now confirmed data, even if the entry
		// started as a reserved default placeholder (the only thing
		// reservedGroups ever holds). Clear the marker and record it as
		// sync-loaded: otherwise the bulk merge could revert it to a
		// pre-modification snapshot, or initReserved could re-synthesize a
		// fresh default over it, silently dropping the just-modified settings
		// from the serving cache while storage keeps the new value.
		delete(cur.reservedGroups, grouppb.Name)
		return true, synced
	})
	return nil
}

// DeleteResourceGroup deletes a resource group.
func (m *Manager) DeleteResourceGroup(keyspaceID uint32, name string) error {
	if !m.writeRole.AllowsMetadataWrite() {
		return errMetadataWriteDisabled
	}
	if err := m.loadResourceGroupIfNeeded(keyspaceID, name); err != nil {
		log.Debug("failed to load resource group", zap.Uint32("keyspace-id", keyspaceID), zap.String("name", name), zap.Error(err))
		return err
	}
	// "default" group can't be deleted, so there is not need to call accessKeyspaceResourceGroupManager
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return errs.ErrKeyspaceNotExists.FastGenByArgs(keyspaceID)
	}
	failpoint.InjectCall("deleteResourceGroupBeforeStorage")
	// Storage phase: validate and remove from storage. Publishing the cache
	// effect is done separately below, against krgm if it's still the live
	// manager for keyspaceID by then, or dropped otherwise (a delete
	// straddling a leadership change) - see publishResourceGroupMutation. The
	// storage removal above already took effect regardless.
	if err := krgm.deleteResourceGroupFromStorage(name); err != nil {
		return err
	}
	m.publishResourceGroupMutation(keyspaceID, name, krgm, func(cur *keyspaceResourceGroupManager) (bool, *ResourceGroup) {
		cur.removeResourceGroupLocked(name)
		return true, nil
	})
	return nil
}

// GetResourceGroup returns a copy of a resource group.
func (m *Manager) GetResourceGroup(keyspaceID uint32, name string, withStats bool) (*ResourceGroup, error) {
	if err := m.loadResourceGroupIfNeeded(keyspaceID, name); err != nil {
		log.Debug("failed to load resource group", zap.Uint32("keyspace-id", keyspaceID), zap.String("name", name), zap.Error(err))
		return nil, err
	}
	krgm, err := m.accessKeyspaceResourceGroupManager(keyspaceID, name)
	if err != nil {
		return nil, err
	}
	return krgm.getResourceGroup(name, withStats), nil
}

// GetMutableResourceGroup returns a mutable resource group.
func (m *Manager) GetMutableResourceGroup(keyspaceID uint32, name string) (*ResourceGroup, error) {
	if err := m.loadResourceGroupIfNeeded(keyspaceID, name); err != nil {
		log.Debug("failed to load resource group", zap.Uint32("keyspace-id", keyspaceID), zap.String("name", name), zap.Error(err))
		return nil, err
	}
	krgm, err := m.accessKeyspaceResourceGroupManager(keyspaceID, name)
	if err != nil {
		return nil, err
	}
	return krgm.getMutableResourceGroup(name), nil
}

// GetResourceGroupList returns copies of resource group list.
// Returns error if resource groups are still being loaded asynchronously.
func (m *Manager) GetResourceGroupList(keyspaceID uint32, withStats bool) ([]*ResourceGroup, error) {
	if !m.isResourceGroupLoadingComplete() {
		log.Debug("resource groups are still being loaded, cannot return list")
		return nil, errs.ErrResourceGroupsLoading
	}
	krgm, err := m.accessKeyspaceResourceGroupManager(keyspaceID, DefaultResourceGroupName)
	if err != nil {
		return nil, err
	}
	return krgm.getResourceGroupList(withStats, true), nil
}

func (m *Manager) persistLoop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(persistLoopInterval)
	failpoint.Inject("fastPersist", func() {
		ticker.Reset(100 * time.Millisecond)
	})
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("resource group manager persist loop exits")
			return
		case <-ticker.C:
			for _, krgm := range m.getKeyspaceResourceGroupManagers() {
				krgm.persistResourceGroupRunningState()
			}
		}
	}
}

func (m *Manager) getKeyspaceResourceGroupManagers() []*keyspaceResourceGroupManager {
	m.RLock()
	defer m.RUnlock()
	krgms := make([]*keyspaceResourceGroupManager, 0, len(m.krgms))
	for _, krgm := range m.krgms {
		krgms = append(krgms, krgm)
	}
	return krgms
}

func (m *Manager) dispatchConsumption(req *rmpb.TokenBucketRequest) error {
	isBackground := req.GetIsBackground()
	isTiFlash := req.GetIsTiflash()
	if isBackground && isTiFlash {
		return errors.New("background and tiflash cannot be true at the same time")
	}
	m.consumptionDispatcher <- &consumptionItem{
		keyspaceID:        ExtractKeyspaceID(req.GetKeyspaceId()),
		resourceGroupName: req.GetResourceGroupName(),
		Consumption:       req.GetConsumptionSinceLastRequest(),
		isBackground:      isBackground,
		isTiFlash:         isTiFlash,
	}
	return nil
}

func (m *Manager) getKeyspaceNameByID(ctx context.Context, id uint32) (string, error) {
	if id == constant.NullKeyspaceID {
		return "", nil
	}
	// Try to get the keyspace name from the cache first.
	m.RLock()
	name, ok := m.keyspaceNameLookup[id]
	m.RUnlock()
	if ok {
		return name, nil
	}
	var loadedName string
	// If the keyspace name is not in the cache, try to get it from the storage.
	err := m.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		meta, err := m.storage.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		loadedName = meta.GetName()
		return nil
	})
	if err != nil {
		log.Error("failed to get the keyspace name", zap.Uint32("keyspace-id", id), zap.Error(err))
		return "", err
	}
	if len(loadedName) == 0 {
		return "", fmt.Errorf("got an empty keyspace name by id %d", id)
	}
	// Update the cache.
	m.updateKeyspaceNameLookup(id, loadedName)
	return loadedName, nil
}

func (m *Manager) updateKeyspaceNameLookup(id uint32, name string) {
	m.Lock()
	defer m.Unlock()
	m.keyspaceNameLookup[id] = name
	m.keyspaceIDLookup[name] = id
}

// GetKeyspaceIDByName gets the keyspace ID by name.
func (m *Manager) GetKeyspaceIDByName(ctx context.Context, name string) (*rmpb.KeyspaceIDValue, error) {
	if len(name) == 0 {
		return &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: constant.NullKeyspaceID}}, nil
	}
	m.RLock()
	id, ok := m.keyspaceIDLookup[name]
	m.RUnlock()
	if ok {
		return &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: id}}, nil
	}
	var (
		loadedID uint32
		err      error
	)
	err = m.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		ok, loadedID, err = m.storage.LoadKeyspaceID(txn, name)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Error("failed to get the keyspace id", zap.String("keyspace-name", name), zap.Error(err))
		return nil, err
	}
	if !ok {
		return nil, errs.ErrKeyspaceNotExistsByName.FastGenByArgs(name)
	}
	// Update the cache.
	m.updateKeyspaceNameLookup(loadedID, name)
	return &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: loadedID}}, nil
}

func (m *Manager) backgroundMetricsFlush(ctx context.Context) {
	defer logutil.LogPanic()
	defer m.wg.Done()
	cleanUpTicker := time.NewTicker(metricsCleanupInterval)
	defer cleanUpTicker.Stop()
	metricsTicker := time.NewTicker(tickPerSecond)
	defer metricsTicker.Stop()
	failpoint.Inject("fastCleanupTicker", func() {
		cleanUpTicker.Reset(100 * time.Millisecond)
	})

	var (
		pushMetricsTicker  *time.Ticker
		pushMetricsTickerC <-chan time.Time
	)
	pushMetricsConfig := pushMetricsConfig{}
	pushMetricsTicker = pushMetricsConfig.syncPushMetricsTicker(
		getPushMetricsConfig(m.GetControllerConfig()),
		pushMetricsTicker,
	)
	if pushMetricsTicker != nil {
		pushMetricsTickerC = pushMetricsTicker.C
	} else {
		pushMetricsTickerC = make(<-chan time.Time)
	}
	defer func() {
		if pushMetricsTicker != nil {
			pushMetricsTicker.Stop()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Info("resource group manager background metrics flush loop exits")
			return
		case consumptionInfo := <-m.consumptionDispatcher:
			if consumptionInfo == nil || consumptionInfo.Consumption == nil {
				continue
			}
			keyspaceID := consumptionInfo.keyspaceID
			keyspaceName, err := m.getKeyspaceNameByID(ctx, keyspaceID)
			if err != nil {
				continue
			}
			consumptionInfo.keyspaceName = keyspaceName
			m.ruCollector.Collect(consumptionInfo)
			m.metrics.recordConsumption(consumptionInfo, m.GetControllerConfig(), time.Now())
			// TODO: maybe we need to distinguish background ru.
			if rg, _ := m.GetMutableResourceGroup(keyspaceID, consumptionInfo.resourceGroupName); rg != nil {
				rg.UpdateRUConsumption(consumptionInfo.Consumption)
			}
		case <-cleanUpTicker.C:
			// Clean up the metrics that have not been updated for a long time.
			for r, lastTime := range m.metrics.consumptionRecordMap {
				if time.Since(lastTime) <= metricsCleanupTimeout {
					continue
				}
				keyspaceName, err := m.getKeyspaceNameByID(ctx, r.keyspaceID)
				if err != nil {
					continue
				}
				m.metrics.cleanupAllMetrics(r, keyspaceName)
				m.ruCollector.remove(keyspaceName)
			}
			// Clean up the stale RU trackers.
			for _, krgm := range m.getKeyspaceResourceGroupManagers() {
				for _, group := range krgm.getResourceGroupList(false, true) {
					grt := krgm.getGroupRUTracker(group.Name)
					if grt == nil {
						continue
					}
					if staleClientUniqueIDs := grt.cleanupStaleRUTrackers(); len(staleClientUniqueIDs) > 0 {
						log.Info("cleaned up stale ru trackers",
							zap.Uint32("keyspace-id", krgm.keyspaceID),
							zap.String("group-name", group.Name),
							zap.Int("stale-client-unique-ids-count", len(staleClientUniqueIDs)),
							zap.Uint64s("stale-client-unique-ids", staleClientUniqueIDs),
						)
					}
				}
			}
		case <-metricsTicker.C:
			// Prevent from holding the lock too long when there're many keyspaces and resource groups.
			for _, krgm := range m.getKeyspaceResourceGroupManagers() {
				// Conciliate the fill rates.
				krgm.conciliateFillRates()
				// Record the metrics.
				keyspaceName, err := m.getKeyspaceNameByID(ctx, krgm.keyspaceID)
				if err != nil {
					continue
				}
				setOrRemoveServiceLimitMetrics(keyspaceName, krgm.getServiceLimiter().getServiceLimit())

				for _, group := range krgm.getResourceGroupList(true, true) {
					groupName := group.Name
					// Record the sum of RRU and WRU every second.
					m.metrics.getMaxPerSecTracker(krgm.keyspaceID, keyspaceName, groupName).flushMetrics()
					metrics := m.metrics.getGaugeMetrics(krgm.keyspaceID, keyspaceName, groupName)
					metrics.setGroup(group, keyspaceName)
					// Record the tracked RU per second.
					if grt := krgm.getGroupRUTracker(groupName); grt != nil {
						metrics.setSampledRUPerSec(grt.getRUPerSec())
					}
				}
			}
			newPushMetricsConfig := getPushMetricsConfig(m.GetControllerConfig())
			if pushMetricsConfig != newPushMetricsConfig {
				pushMetricsTicker = pushMetricsConfig.syncPushMetricsTicker(
					newPushMetricsConfig,
					pushMetricsTicker,
				)
				if pushMetricsTicker != nil {
					pushMetricsTickerC = pushMetricsTicker.C
					log.Info("push metrics ticker updated", zap.Duration("interval", pushMetricsConfig.interval))
				} else {
					pushMetricsTickerC = make(<-chan time.Time)
				}
			}
		case <-pushMetricsTickerC:
			if pushMetricsConfig.address == "" {
				continue
			}
			podName := os.Getenv("HOSTNAME")
			if podName == "" {
				podName = "default"
			}
			pushCtx, cancel := context.WithTimeout(ctx, pushMetricsTimeout)
			start := time.Now()
			err := push.New(pushMetricsConfig.address, "resource_group_svc").
				Grouping("pod", podName).
				Collector(readRequestUnitCost).
				Collector(writeRequestUnitCost).
				Collector(sqlLayerRequestUnitCost).
				PushContext(pushCtx)
			cancel()
			if err != nil {
				log.Warn("push metrics to Prometheus failed", zap.Error(err))
			}
			pushRUMetricsDuration.Observe(time.Since(start).Seconds())
		}
	}
}
