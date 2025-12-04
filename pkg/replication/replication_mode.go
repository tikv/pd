// Copyright 2020 TiKV Project Authors.
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

package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/pdpb"
	pb "github.com/pingcap/kvproto/pkg/replication_modepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/server/config"
)

const (
	modeMajority                 = "majority"
	modeDRAutoSync               = "dr-auto-sync"
	defaultDRTiKVSyncTimeoutHint = time.Minute
)

func modeToPB(m string) pb.ReplicationMode {
	switch m {
	case modeMajority:
		return pb.ReplicationMode_MAJORITY
	case modeDRAutoSync:
		return pb.ReplicationMode_DR_AUTO_SYNC
	}
	return 0
}

// FileReplicater is the interface that can save important data to all cluster
// nodes.
type FileReplicater interface {
	GetMembers() ([]*pdpb.Member, error)
	ReplicateFileToMember(ctx context.Context, member *pdpb.Member, name string, data []byte) error
}

// DrStatusFile is the file name that stores the dr status.
const DrStatusFile = "DR_STATE"
const persistFileTimeout = time.Second * 3

// ModeManager is used to control how raft logs are synchronized between
// different tikv nodes.
type ModeManager struct {
	initTime time.Time

	syncutil.RWMutex
	config         config.ReplicationModeConfig
	storage        endpoint.ReplicationStatusStorage
	cluster        sche.ClusterInformer
	fileReplicater FileReplicater
	replicateState sync.Map

	drAutoSync drAutoSyncStatus
	// intermediate states of the recovery process
	// they are accessed without locks as they are only used by background job.
	drRecoverKey   []byte // all regions that has startKey < drRecoverKey are successfully recovered
	drRecoverCount int    // number of regions that has startKey < drRecoverKey
	// When find a region that is not recovered, PD will not check all the
	// remaining regions, but read a region to estimate the overall progress
	drSampleRecoverCount int // number of regions that are recovered in sample
	drSampleTotalRegion  int // number of regions in sample
	drTotalRegion        int // number of all regions

	drStoreStatus sync.Map
}

// NewReplicationModeManager creates the replicate mode manager.
func NewReplicationModeManager(config config.ReplicationModeConfig, storage endpoint.ReplicationStatusStorage, cluster sche.ClusterInformer, fileReplicater FileReplicater) (*ModeManager, error) {
	m := &ModeManager{
		initTime:       time.Now(),
		config:         config,
		storage:        storage,
		cluster:        cluster,
		fileReplicater: fileReplicater,
	}
	switch config.ReplicationMode {
	case modeMajority:
	case modeDRAutoSync:
		if err := m.loadDRAutoSync(); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// UpdateConfig updates configuration online and updates internal state.
func (m *ModeManager) UpdateConfig(config config.ReplicationModeConfig) error {
	m.Lock()
	defer m.Unlock()
	// If mode change from 'majority' to 'dr-auto-sync', or label key is updated switch to 'sync_recover'.
	if m.config.ReplicationMode == modeMajority && config.ReplicationMode == modeDRAutoSync {
		old := m.config
		m.config = config
		err := m.drSwitchToSyncRecoverWithLock()
		if err != nil {
			// restore
			m.config = old
		}
		return err
	}
	m.config = config
	return nil
}

// GetReplicationStatus returns the status to sync with tikv servers.
func (m *ModeManager) GetReplicationStatus() *pb.ReplicationStatus {
	m.RLock()
	defer m.RUnlock()

	p := &pb.ReplicationStatus{
		Mode: modeToPB(m.config.ReplicationMode),
	}
	switch m.config.ReplicationMode {
	case modeMajority:
	case modeDRAutoSync:
		p.DrAutoSync = &pb.DRAutoSync{
			LabelKey: m.config.DRAutoSync.LabelKey,
			State:    pb.DRAutoSyncState(pb.DRAutoSyncState_value[strings.ToUpper(m.drAutoSync.State)]),
			StateId:  m.drAutoSync.StateID,
			// TODO: make it works, ref https://github.com/tikv/tikv/issues/7945
			WaitSyncTimeoutHint: int32(defaultDRTiKVSyncTimeoutHint.Seconds()),
			AvailableStores:     m.drAutoSync.AvailableStores,
			PauseRegionSplit:    m.config.DRAutoSync.PauseRegionSplit && m.drAutoSync.State != drStateSync,
		}
	}
	return p
}

// IsRegionSplitPaused returns true if region split need be paused.
func (m *ModeManager) IsRegionSplitPaused() bool {
	m.RLock()
	defer m.RUnlock()
	return m.config.ReplicationMode == modeDRAutoSync &&
		m.config.DRAutoSync.PauseRegionSplit &&
		m.drAutoSync.State != drStateSync
}

// HTTPReplicationStatus is for query status from HTTP API.
type HTTPReplicationStatus struct {
	Mode       string `json:"mode"`
	DrAutoSync struct {
		LabelKey        string  `json:"label_key"`
		State           string  `json:"state"`
		StateID         uint64  `json:"state_id,omitempty"`
		ACIDConsistent  bool    `json:"acid_consistent"`
		TotalRegions    int     `json:"total_regions,omitempty"`
		SyncedRegions   int     `json:"synced_regions,omitempty"`
		RecoverProgress float32 `json:"recover_progress,omitempty"`
	} `json:"dr-auto-sync,omitempty"`
}

// GetReplicationStatusHTTP returns status for HTTP API.
func (m *ModeManager) GetReplicationStatusHTTP() *HTTPReplicationStatus {
	m.RLock()
	defer m.RUnlock()
	var status HTTPReplicationStatus
	status.Mode = m.config.ReplicationMode
	switch status.Mode {
	case modeMajority:
	case modeDRAutoSync:
		status.DrAutoSync.LabelKey = m.config.DRAutoSync.LabelKey
		status.DrAutoSync.State = m.drAutoSync.State
		status.DrAutoSync.StateID = m.drAutoSync.StateID
		status.DrAutoSync.ACIDConsistent = m.drAutoSync.State != drStateSyncRecover
		status.DrAutoSync.RecoverProgress = m.drAutoSync.RecoverProgress
		status.DrAutoSync.TotalRegions = m.drAutoSync.TotalRegions
		status.DrAutoSync.SyncedRegions = m.drAutoSync.SyncedRegions
	}
	return &status
}

func (m *ModeManager) getModeName() string {
	m.RLock()
	defer m.RUnlock()
	return m.config.ReplicationMode
}

const (
	drStateSync        = "sync"
	drStateAsyncWait   = "async_wait"
	drStateAsync       = "async"
	drStateSyncRecover = "sync_recover"
)

type drAutoSyncStatus struct {
	State            string     `json:"state,omitempty"`
	StateID          uint64     `json:"state_id,omitempty"`
	AsyncStartTime   *time.Time `json:"async_start,omitempty"`
	RecoverStartTime *time.Time `json:"recover_start,omitempty"`
	TotalRegions     int        `json:"total_regions,omitempty"`
	SyncedRegions    int        `json:"synced_regions,omitempty"`
	RecoverProgress  float32    `json:"recover_progress,omitempty"`
	AvailableStores  []uint64   `json:"available_stores,omitempty"`
}

func (m *ModeManager) loadDRAutoSync() error {
	ok, err := m.storage.LoadReplicationStatus(modeDRAutoSync, &m.drAutoSync)
	if err != nil {
		return err
	}
	if !ok {
		// initialize
		return m.drSwitchToSync()
	}
	return nil
}

func (m *ModeManager) drSwitchToAsyncWait(availableStores []uint64) error {
	m.Lock()
	defer m.Unlock()

	id, _, err := m.cluster.AllocID(1)
	if err != nil {
		log.Warn("failed to switch to async wait state", zap.String("replicate-mode", modeDRAutoSync), errs.ZapError(err))
		return err
	}
	dr := drAutoSyncStatus{State: drStateAsyncWait, StateID: id, AvailableStores: availableStores}
	if err := m.storage.SaveReplicationStatus(modeDRAutoSync, dr); err != nil {
		log.Warn("failed to switch to async state", zap.String("replicate-mode", modeDRAutoSync), errs.ZapError(err))
		return err
	}
	m.drAutoSync = dr
	log.Info("switched to async_wait state", zap.String("replicate-mode", modeDRAutoSync))
	return nil
}

func (m *ModeManager) drSwitchToAsync(availableStores []uint64) error {
	m.Lock()
	defer m.Unlock()
	return m.drSwitchToAsyncWithLock(availableStores)
}

func (m *ModeManager) drSwitchToAsyncWithLock(availableStores []uint64) error {
	id, _, err := m.cluster.AllocID(1)
	if err != nil {
		log.Warn("failed to switch to async state", zap.String("replicate-mode", modeDRAutoSync), errs.ZapError(err))
		return err
	}
	now := time.Now()
	dr := drAutoSyncStatus{State: drStateAsync, StateID: id, AvailableStores: availableStores, AsyncStartTime: &now}
	if err := m.storage.SaveReplicationStatus(modeDRAutoSync, dr); err != nil {
		log.Warn("failed to switch to async state", zap.String("replicate-mode", modeDRAutoSync), errs.ZapError(err))
		return err
	}
	m.drAutoSync = dr
	log.Info("switched to async state", zap.String("replicate-mode", modeDRAutoSync))
	return nil
}

func (m *ModeManager) drDurationSinceAsyncStart() time.Duration {
	m.RLock()
	defer m.RUnlock()
	if m.drAutoSync.AsyncStartTime == nil {
		return 0
	}
	return time.Since(*m.drAutoSync.AsyncStartTime)
}

func (m *ModeManager) drSwitchToSyncRecover() error {
	m.Lock()
	defer m.Unlock()
	return m.drSwitchToSyncRecoverWithLock()
}

func (m *ModeManager) drSwitchToSyncRecoverWithLock() error {
	id, _, err := m.cluster.AllocID(1)
	if err != nil {
		log.Warn("failed to switch to sync_recover state", zap.String("replicate-mode", modeDRAutoSync), errs.ZapError(err))
		return err
	}
	now := time.Now()
	dr := drAutoSyncStatus{State: drStateSyncRecover, StateID: id, RecoverStartTime: &now}
	if err = m.storage.SaveReplicationStatus(modeDRAutoSync, dr); err != nil {
		log.Warn("failed to switch to sync_recover state", zap.String("replicate-mode", modeDRAutoSync), errs.ZapError(err))
		return err
	}
	m.drAutoSync = dr
	m.drRecoverKey, m.drRecoverCount = nil, 0
	log.Info("switched to sync_recover state", zap.String("replicate-mode", modeDRAutoSync))
	return nil
}

func (m *ModeManager) drSwitchToSync() error {
	m.Lock()
	defer m.Unlock()
	id, _, err := m.cluster.AllocID(1)
	if err != nil {
		log.Warn("failed to switch to sync state", zap.String("replicate-mode", modeDRAutoSync), errs.ZapError(err))
		return err
	}
	dr := drAutoSyncStatus{State: drStateSync, StateID: id}
	if err := m.storage.SaveReplicationStatus(modeDRAutoSync, dr); err != nil {
		log.Warn("failed to switch to sync state", zap.String("replicate-mode", modeDRAutoSync), errs.ZapError(err))
		return err
	}
	m.drAutoSync = dr
	log.Info("switched to sync state", zap.String("replicate-mode", modeDRAutoSync))
	return nil
}

func (m *ModeManager) drGetState() string {
	m.RLock()
	defer m.RUnlock()
	return m.drAutoSync.State
}

const (
	idleTimeout            = time.Minute
	tickInterval           = 500 * time.Millisecond
	replicateStateInterval = time.Second * 5
)

// Run starts the background job.
func (m *ModeManager) Run(ctx context.Context) {
	// Wait for a while when just start, in case tikv do not connect in time.
	timer := time.NewTimer(idleTimeout)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		log.Info("replication mode manager is stopped")
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.tickUpdateState()
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		defer func() {
			wg.Done()
			drStateGauge.Set(0)
		}()
		ticker := time.NewTicker(replicateStateInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.tickReplicateStatus()
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
	log.Info("replication mode manager is stopped")
}

func minimalUpVoters(rule *placement.Rule, upStores, downStores []*core.StoreInfo) int {
	if rule.Role == placement.Learner {
		return 0
	}
	var up, down int
	for _, s := range upStores {
		if placement.MatchLabelConstraints(s, rule.LabelConstraints) {
			up++
		}
	}
	for _, s := range downStores {
		if placement.MatchLabelConstraints(s, rule.LabelConstraints) {
			down++
		}
	}
	minimalUp := rule.Count - down
	if minimalUp < 0 {
		minimalUp = 0
	}
	if minimalUp > up {
		minimalUp = up
	}
	return minimalUp
}

func (m *ModeManager) tickUpdateState() {
	if m.getModeName() != modeDRAutoSync {
		return
	}

	drTickCounter.Inc()

	stores, storeIDs := m.checkStoreStatus()

	var primaryHasVoter, drHasVoter bool
	var totalVoter, totalUpVoter int
	for _, r := range m.cluster.GetRuleManager().GetAllRules() {
		if len(r.StartKey) > 0 || len(r.EndKey) > 0 {
			// All rules should be global rules. If not, skip it.
			continue
		}
		if r.Role != placement.Learner {
			totalVoter += r.Count
		}
		minimalUpPrimary := minimalUpVoters(r, stores[primaryUp], stores[primaryDown])
		minimalUpDr := minimalUpVoters(r, stores[drUp], stores[drDown])
		primaryHasVoter = primaryHasVoter || minimalUpPrimary > 0
		drHasVoter = drHasVoter || minimalUpDr > 0
		upVoters := minimalUpPrimary + minimalUpDr
		if upVoters > r.Count {
			upVoters = r.Count
		}
		totalUpVoter += upVoters
	}

	// canSync is true when every region has at least 1 voter replica in each DC.
	// hasMajority is true when every region has majority peer online.
	canSync := primaryHasVoter && drHasVoter
	hasMajority := totalUpVoter*2 > totalVoter

	/*

	           +----+      all region sync     +------------+
	           |SYNC| <----------------------- |SYNC_RECOVER|
	           +----+                          +------------+
	             |^                                 ^ |
	      DR down||                                 | |DR down
	             ||DR up                       DR up| |
	             v|                                 | v
	        +----------+     all tikv sync        +-----+
	   +--> |ASYNC_WAIT|------------------------> |ASYNC|<------+
	   |    +----------+                          +-----+       |
	   |            |                              |            |
	   |tikv up/down|                              |tikv up/down|
	   +------------+                              +------------+

	*/

	state := m.drGetState()
	switch state {
	case drStateSync:
		// If hasMajority is false, the cluster is always unavailable. Switch to async won't help.
		if !canSync && hasMajority {
			_ = m.drSwitchToAsyncWait(storeIDs[primaryUp])
		}
	case drStateAsyncWait:
		if canSync {
			_ = m.drSwitchToSync()
			break
		}
		if oldAvailableStores := m.drGetAvailableStores(); !reflect.DeepEqual(oldAvailableStores, storeIDs[primaryUp]) {
			_ = m.drSwitchToAsyncWait(storeIDs[primaryUp])
			break
		}
		if m.drCheckStoreStateUpdated(storeIDs[primaryUp]) {
			_ = m.drSwitchToAsync(storeIDs[primaryUp])
		}
	case drStateAsync:
		if canSync && m.drDurationSinceAsyncStart() > m.config.DRAutoSync.WaitRecoverTimeout.Duration {
			_ = m.drSwitchToSyncRecover()
			break
		}
		if !reflect.DeepEqual(m.drGetAvailableStores(), storeIDs[primaryUp]) && m.drCheckStoreStateUpdated(storeIDs[primaryUp]) {
			_ = m.drSwitchToAsync(storeIDs[primaryUp])
		}
	case drStateSyncRecover:
		if !canSync && hasMajority {
			_ = m.drSwitchToAsync(storeIDs[primaryUp])
		} else {
			m.updateProgress()
			progress := m.estimateProgress()
			drRecoveredRegionGauge.Set(float64(m.drRecoverCount))
			drRecoverProgressGauge.Set(float64(progress))

			if progress == 1.0 {
				_ = m.drSwitchToSync()
			} else {
				m.updateRecoverProgress(progress)
			}
		}
	}

	logFunc := log.Debug
	if state != m.drGetState() {
		logFunc = log.Info
	}
	logFunc("replication store status",
		zap.Uint64s("up-primary", storeIDs[primaryUp]),
		zap.Uint64s("up-dr", storeIDs[drUp]),
		zap.Uint64s("down-primary", storeIDs[primaryDown]),
		zap.Uint64s("down-dr", storeIDs[drDown]),
		zap.Bool("can-sync", canSync),
		zap.Bool("has-majority", hasMajority),
	)
}

func (m *ModeManager) tickReplicateStatus() {
	if m.getModeName() != modeDRAutoSync {
		return
	}

	m.RLock()
	state := drAutoSyncStatus{
		State:            m.drAutoSync.State,
		StateID:          m.drAutoSync.StateID,
		AvailableStores:  m.drAutoSync.AvailableStores,
		RecoverStartTime: m.drAutoSync.RecoverStartTime,
	}
	m.RUnlock()

	// recording metrics
	var stateNumber float64
	switch state.State {
	case drStateSync:
		stateNumber = 1
	case drStateAsyncWait:
		stateNumber = 2
	case drStateAsync:
		stateNumber = 3
	case drStateSyncRecover:
		stateNumber = 4
	}
	drStateGauge.Set(stateNumber)
	drStateIDGauge.Set(float64(state.StateID))

	data, _ := json.Marshal(state)

	members, err := m.fileReplicater.GetMembers()
	if err != nil {
		log.Warn("failed to get members", zap.String("replicate-mode", modeDRAutoSync))
		return
	}
	for _, member := range members {
		stateID, ok := m.replicateState.Load(member.GetMemberId())
		if !ok || stateID.(uint64) != state.StateID {
			ctx, cancel := context.WithTimeout(context.Background(), persistFileTimeout)
			err := m.fileReplicater.ReplicateFileToMember(ctx, member, DrStatusFile, data)
			if err != nil {
				log.Warn("failed to switch state", zap.String("replicate-mode", modeDRAutoSync), zap.String("new-state", state.State), errs.ZapError(err))
			} else {
				m.replicateState.Store(member.GetMemberId(), state.StateID)
			}
			cancel()
		}
	}
}

const (
	primaryUp = iota
	primaryDown
	drUp
	drDown
	storeStatusTypeCount
)

func (m *ModeManager) checkStoreStatus() ([][]*core.StoreInfo, [][]uint64) {
	m.RLock()
	defer m.RUnlock()
	stores, storeIDs := make([][]*core.StoreInfo, storeStatusTypeCount), make([][]uint64, storeStatusTypeCount)
	for _, s := range m.cluster.GetStores() {
		if s.IsRemoved() {
			continue
		}
		down := s.DownTime() >= m.config.DRAutoSync.WaitStoreTimeout.Duration
		labelValue := s.GetLabelValue(m.config.DRAutoSync.LabelKey)
		if labelValue == m.config.DRAutoSync.Primary {
			if down {
				stores[primaryDown] = append(stores[primaryDown], s)
				storeIDs[primaryDown] = append(storeIDs[primaryDown], s.GetID())
			} else {
				stores[primaryUp] = append(stores[primaryUp], s)
				storeIDs[primaryUp] = append(storeIDs[primaryUp], s.GetID())
			}
		}
		if labelValue == m.config.DRAutoSync.DR {
			if down {
				stores[drDown] = append(stores[drDown], s)
				storeIDs[drDown] = append(storeIDs[drDown], s.GetID())
			} else {
				stores[drUp] = append(stores[drUp], s)
				storeIDs[drUp] = append(storeIDs[drUp], s.GetID())
			}
		}
	}
	for i := range stores {
		sort.Slice(stores[i], func(a, b int) bool { return stores[i][a].GetID() < stores[i][b].GetID() })
		sort.Slice(storeIDs[i], func(a, b int) bool { return storeIDs[i][a] < storeIDs[i][b] })
	}
	return stores, storeIDs
}

// UpdateStoreDRStatus saves the dr-autosync status of a store.
func (m *ModeManager) UpdateStoreDRStatus(id uint64, status *pb.StoreDRAutoSyncStatus) {
	m.drStoreStatus.Store(id, status)
}

func (m *ModeManager) drGetAvailableStores() []uint64 {
	m.RLock()
	defer m.RUnlock()
	return m.drAutoSync.AvailableStores
}

func (m *ModeManager) drCheckStoreStateUpdated(stores []uint64) bool {
	state := m.GetReplicationStatus().GetDrAutoSync()
	for _, s := range stores {
		status, ok := m.drStoreStatus.Load(s)
		if !ok {
			return false
		}
		drStatus := status.(*pb.StoreDRAutoSyncStatus)
		if drStatus.GetState() != state.GetState() || drStatus.GetStateId() != state.GetStateId() {
			return false
		}
	}
	return true
}

var (
	regionScanBatchSize = 1024
	regionMinSampleSize = 512
)

func (m *ModeManager) updateProgress() {
	m.RLock()
	defer m.RUnlock()

	for len(m.drRecoverKey) > 0 || m.drRecoverCount == 0 {
		regions := m.cluster.ScanRegions(m.drRecoverKey, nil, regionScanBatchSize)
		if len(regions) == 0 {
			log.Warn("scan empty regions",
				logutil.ZapRedactByteString("recover-key", m.drRecoverKey))
			return
		}
		for i, r := range regions {
			if m.checkRegionRecover(r, m.drRecoverKey) {
				m.drRecoverKey = r.GetEndKey()
				m.drRecoverCount++
				continue
			}
			// take sample and quit iteration.
			sampleRegions := regions[i:]
			if len(sampleRegions) < regionMinSampleSize {
				if last := sampleRegions[len(sampleRegions)-1]; len(last.GetEndKey()) > 0 {
					sampleRegions = append(sampleRegions, m.cluster.ScanRegions(last.GetEndKey(), nil, regionMinSampleSize)...)
				}
			}
			m.drSampleRecoverCount = 0
			key := m.drRecoverKey
			for _, r := range sampleRegions {
				if m.checkRegionRecover(r, key) {
					m.drSampleRecoverCount++
				}
				key = r.GetEndKey()
			}
			m.drSampleTotalRegion = len(sampleRegions)
			m.drTotalRegion = m.cluster.GetTotalRegionCount()
			return
		}
	}
}

func (m *ModeManager) estimateProgress() float32 {
	if len(m.drRecoverKey) == 0 && m.drRecoverCount > 0 {
		return 1.0
	}

	// make sure progress less than 1
	if m.drSampleTotalRegion <= m.drSampleRecoverCount {
		m.drSampleTotalRegion = m.drSampleRecoverCount + 1
	}
	totalUnchecked := m.drTotalRegion - m.drRecoverCount
	if totalUnchecked < m.drSampleTotalRegion {
		totalUnchecked = m.drSampleTotalRegion
	}
	total := m.drRecoverCount + totalUnchecked
	uncheckRecovered := float32(totalUnchecked) * float32(m.drSampleRecoverCount) / float32(m.drSampleTotalRegion)
	return (float32(m.drRecoverCount) + uncheckRecovered) / float32(total)
}

func (m *ModeManager) checkRegionRecover(region *core.RegionInfo, startKey []byte) bool {
	// if the region not contains the key, log it and return false
	if bytes.Compare(startKey, region.GetStartKey()) < 0 {
		log.Warn("found region gap",
			logutil.ZapRedactByteString("key", core.HexRegionKey(startKey)),
			logutil.ZapRedactStringer("region", core.RegionToHexMeta(region.GetMeta())),
			zap.Uint64("region-id", region.GetID()))
		return false
	}
	return region.GetReplicationStatus().GetStateId() == m.drAutoSync.StateID &&
		region.GetReplicationStatus().GetState() == pb.RegionReplicationState_INTEGRITY_OVER_LABEL
}

func (m *ModeManager) updateRecoverProgress(progress float32) {
	m.Lock()
	defer m.Unlock()
	m.drAutoSync.RecoverProgress = progress
	m.drAutoSync.TotalRegions = m.drTotalRegion
	m.drAutoSync.SyncedRegions = m.drRecoverCount
}
