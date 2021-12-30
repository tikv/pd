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

	"github.com/pingcap/kvproto/pkg/pdpb"
	pb "github.com/pingcap/kvproto/pkg/replication_modepb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
)

const (
	modeMajority   = "majority"
	modeDRAutoSync = "dr-auto-sync"
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

const drStatusFile = "DR_STATE"
const persistFileTimeout = time.Second * 10

// ModeManager is used to control how raft logs are synchronized between
// different tikv nodes.
type ModeManager struct {
	initTime time.Time

	sync.RWMutex
	config            config.ReplicationModeConfig
	storage           *core.Storage
	cluster           opt.Cluster
	fileReplicater    FileReplicater
	replicatedMembers []uint64

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

	drMemberWaitAsyncTime map[uint64]time.Time // last sync time with follower nodes
	drStoreStatus         sync.Map
}

// NewReplicationModeManager creates the replicate mode manager.
func NewReplicationModeManager(config config.ReplicationModeConfig, storage *core.Storage, cluster opt.Cluster, fileReplicater FileReplicater) (*ModeManager, error) {
	m := &ModeManager{
		initTime:              time.Now(),
		config:                config,
		storage:               storage,
		cluster:               cluster,
		fileReplicater:        fileReplicater,
		drMemberWaitAsyncTime: make(map[uint64]time.Time),
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

// UpdateMemberWaitAsyncTime updates a member's wait async time.
func (m *ModeManager) UpdateMemberWaitAsyncTime(memberID uint64) {
	m.Lock()
	defer m.Unlock()
	t := time.Now()
	log.Info("udpate member wait async time", zap.Uint64("memberID", memberID), zap.Time("time", t))
	m.drMemberWaitAsyncTime[memberID] = t
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
			LabelKey:            m.config.DRAutoSync.LabelKey,
			State:               pb.DRAutoSyncState(pb.DRAutoSyncState_value[strings.ToUpper(m.drAutoSync.State)]),
			StateId:             m.drAutoSync.StateID,
			WaitSyncTimeoutHint: int32(m.config.DRAutoSync.WaitSyncTimeout.Seconds()),
			AvailableStores:     m.drAutoSync.AvailableStores,
		}
	}
	return p
}

// HTTPReplicationStatus is for query status from HTTP API.
type HTTPReplicationStatus struct {
	Mode       string `json:"mode"`
	DrAutoSync struct {
		LabelKey        string  `json:"label_key"`
		State           string  `json:"state"`
		StateID         uint64  `json:"state_id,omitempty"`
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

func (m *ModeManager) drCheckAsyncTimeout() bool {
	m.RLock()
	defer m.RUnlock()
	timeout := m.config.DRAutoSync.WaitAsyncTimeout.Duration
	if timeout == 0 {
		return true
	}
	// make sure all members are timeout.
	for _, t := range m.drMemberWaitAsyncTime {
		if time.Since(t) <= timeout {
			return false
		}
	}
	// make sure all members that have synced with previous leader are timeout.
	return time.Since(m.initTime) > timeout
}

func (m *ModeManager) drSwitchToAsyncWait(availableStores []uint64) error {
	m.Lock()
	defer m.Unlock()

	id, err := m.cluster.AllocID()
	if err != nil {
		log.Warn("failed to switch to async wait state", zap.String("replicate-mode", modeDRAutoSync), errs.ZapError(err))
		return err
	}
	dr := drAutoSyncStatus{State: drStateAsyncWait, StateID: id, AvailableStores: availableStores}
	m.drPersistStatusWithLock(dr)
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
	id, err := m.cluster.AllocID()
	if err != nil {
		log.Warn("failed to switch to async state", zap.String("replicate-mode", modeDRAutoSync), errs.ZapError(err))
		return err
	}
	dr := drAutoSyncStatus{State: drStateAsync, StateID: id, AvailableStores: availableStores}
	m.drPersistStatusWithLock(dr)
	if err := m.storage.SaveReplicationStatus(modeDRAutoSync, dr); err != nil {
		log.Warn("failed to switch to async state", zap.String("replicate-mode", modeDRAutoSync), errs.ZapError(err))
		return err
	}
	m.drAutoSync = dr
	log.Info("switched to async state", zap.String("replicate-mode", modeDRAutoSync))
	return nil
}

func (m *ModeManager) drSwitchToSyncRecover() error {
	m.Lock()
	defer m.Unlock()
	return m.drSwitchToSyncRecoverWithLock()
}

func (m *ModeManager) drSwitchToSyncRecoverWithLock() error {
	id, err := m.cluster.AllocID()
	if err != nil {
		log.Warn("failed to switch to sync_recover state", zap.String("replicate-mode", modeDRAutoSync), errs.ZapError(err))
		return err
	}
	now := time.Now()
	dr := drAutoSyncStatus{State: drStateSyncRecover, StateID: id, RecoverStartTime: &now}
	m.drPersistStatusWithLock(dr)
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
	id, err := m.cluster.AllocID()
	if err != nil {
		log.Warn("failed to switch to sync state", zap.String("replicate-mode", modeDRAutoSync), errs.ZapError(err))
		return err
	}
	dr := drAutoSyncStatus{State: drStateSync, StateID: id}
	m.drPersistStatusWithLock(dr)
	if err := m.storage.SaveReplicationStatus(modeDRAutoSync, dr); err != nil {
		log.Warn("failed to switch to sync state", zap.String("replicate-mode", modeDRAutoSync), errs.ZapError(err))
		return err
	}
	m.drAutoSync = dr
	log.Info("switched to sync state", zap.String("replicate-mode", modeDRAutoSync))
	return nil
}

func (m *ModeManager) drPersistStatusWithLock(status drAutoSyncStatus) {
	ctx, cancel := context.WithTimeout(context.Background(), persistFileTimeout)
	defer cancel()

	members, err := m.fileReplicater.GetMembers()
	if err != nil {
		log.Warn("failed to get members", zap.String("replicate-mode", modeDRAutoSync))
		return
	}

	data, _ := json.Marshal(status)

	m.replicatedMembers = m.replicatedMembers[:0]
	for _, member := range members {
		if err := m.fileReplicater.ReplicateFileToMember(ctx, member, drStatusFile, data); err != nil {
			log.Warn("failed to switch state", zap.String("replicate-mode", modeDRAutoSync), zap.String("new-state", status.State), errs.ZapError(err))
			// Throw away the error to make it possible to switch to async when
			// primary and dr DC are disconnected. This will result in the
			// inability to accurately determine whether data is fully
			// synchronized when using dr DC to disaster recovery.
			// Since the member will not be in `replicatedMembers` list, PD will
			// try to replicate state file later.
		} else {
			m.replicatedMembers = append(m.replicatedMembers, member.GetMemberId())
		}
	}
}

func (m *ModeManager) drCheckNeedPersistStatus(members []*pdpb.Member) bool {
	m.RLock()
	defer m.RUnlock()
	return slice.AnyOf(members, func(i int) bool { // if there is any member in the new list
		return slice.NoneOf(m.replicatedMembers, func(j int) bool { // not replicated
			return m.replicatedMembers[j] == members[i].GetMemberId()
		})
	})
}

func (m *ModeManager) drPersistStatus() {
	m.Lock()
	defer m.Unlock()
	m.drPersistStatusWithLock(drAutoSyncStatus{State: m.drAutoSync.State, StateID: m.drAutoSync.StateID})
}

func (m *ModeManager) drGetState() string {
	m.RLock()
	defer m.RUnlock()
	return m.drAutoSync.State
}

const (
	idleTimeout  = time.Minute
	tickInterval = time.Second * 10
)

// Run starts the background job.
func (m *ModeManager) Run(ctx context.Context) {
	// Wait for a while when just start, in case tikv do not connect in time.
	select {
	case <-time.After(idleTimeout):
	case <-ctx.Done():
		return
	}
	for {
		select {
		case <-time.After(tickInterval):
		case <-ctx.Done():
			return
		}
		m.tickDR()
	}
}

func (m *ModeManager) tickDR() {
	if m.getModeName() != modeDRAutoSync {
		return
	}

	drTickCounter.Inc()

	totalPrimaryPeers, totalDrPeers := m.config.DRAutoSync.PrimaryReplicas, m.config.DRAutoSync.DRReplicas
	stores := m.checkStoreStatus()

	// canSync is true when every region has at least 1 replica in each DC.
	canSync := len(stores[primaryDown]) < totalPrimaryPeers && len(stores[drDown]) < totalDrPeers &&
		len(stores[primaryUp]) > 0 && len(stores[drUp]) > 0

	// hasMajority is true when every region has majority peer online.
	var upPeers int
	if len(stores[primaryDown]) < totalPrimaryPeers {
		upPeers += totalPrimaryPeers - len(stores[primaryDown])
	}
	if len(stores[drDown]) < totalDrPeers {
		upPeers += totalDrPeers - len(stores[drDown])
	}
	hasMajority := upPeers*2 > totalPrimaryPeers+totalDrPeers

	log.Debug("replication store status",
		zap.Uint64s("up-primary", stores[primaryUp]),
		zap.Uint64s("up-dr", stores[drUp]),
		zap.Uint64s("down-primary", stores[primaryDown]),
		zap.Uint64s("down-dr", stores[drDown]),
		zap.Bool("can-sync", canSync),
		zap.Int("up-peers", upPeers),
		zap.Bool("has-majority", hasMajority),
	)

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

	switch m.drGetState() {
	case drStateSync:
		// If hasMajority is false, the cluster is always unavailable. Switch to async won't help.
		if !canSync && hasMajority && m.drCheckAsyncTimeout() {
			m.drSwitchToAsyncWait(stores[primaryUp])
		}
	case drStateAsyncWait:
		if canSync {
			m.drSwitchToSync()
			break
		}
		if oldAvailableStores := m.drGetAvailableStores(); !reflect.DeepEqual(oldAvailableStores, stores[primaryUp]) {
			m.drSwitchToAsyncWait(stores[primaryUp])
			break
		}
		if m.drCheckStoreStateUpdated(stores[primaryUp]) {
			m.drSwitchToAsync(stores[primaryUp])
		}
	case drStateAsync:
		if canSync {
			m.drSwitchToSyncRecover()
			break
		}
		if !reflect.DeepEqual(m.drGetAvailableStores(), stores[primaryUp]) && m.drCheckStoreStateUpdated(stores[primaryUp]) {
			m.drSwitchToAsync(stores[primaryUp])
		}
	case drStateSyncRecover:
		if !canSync {
			m.drSwitchToAsync(stores[primaryUp])
		} else {
			m.updateProgress()
			progress := m.estimateProgress()
			drRecoverProgressGauge.Set(float64(progress))

			if progress == 1.0 {
				m.drSwitchToSync()
			} else {
				m.updateRecoverProgress(progress)
			}
		}
	}

	m.checkReplicateFile()
}

const (
	primaryUp = iota
	primaryDown
	drUp
	drDown
	storeStatusTypeCount
)

func (m *ModeManager) checkStoreStatus() [][]uint64 {
	m.RLock()
	defer m.RUnlock()
	stores := make([][]uint64, storeStatusTypeCount)
	for _, s := range m.cluster.GetStores() {
		if s.IsTombstone() {
			continue
		}
		down := s.DownTime() >= m.config.DRAutoSync.WaitStoreTimeout.Duration
		labelValue := s.GetLabelValue(m.config.DRAutoSync.LabelKey)
		if labelValue == m.config.DRAutoSync.Primary {
			if down {
				stores[primaryDown] = append(stores[primaryDown], s.GetID())
			} else {
				stores[primaryUp] = append(stores[primaryUp], s.GetID())
			}
		}
		if labelValue == m.config.DRAutoSync.DR {
			if down {
				stores[drDown] = append(stores[drDown], s.GetID())
			} else {
				stores[drUp] = append(stores[drUp], s.GetID())
			}
		}
	}
	for i := range stores {
		sort.Slice(stores[i], func(a, b int) bool { return stores[i][a] < stores[i][b] })
	}
	return stores
}

// UpdateStoreStatus saves the dr-autosync status of a store.
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

func (m *ModeManager) checkReplicateFile() {
	members, err := m.fileReplicater.GetMembers()
	if err != nil {
		log.Warn("failed to get members", zap.String("replicate-mode", modeDRAutoSync))
		return
	}
	if m.drCheckNeedPersistStatus(members) {
		m.drPersistStatus()
	}
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
			m.drTotalRegion = m.cluster.GetRegionCount()
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
