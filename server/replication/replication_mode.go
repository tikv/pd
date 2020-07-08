// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	pb "github.com/pingcap/kvproto/pkg/replication_modepb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/typeutil"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/member"
	"github.com/pingcap/pd/v4/server/schedule/opt"
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
	GetDrAutoSyncStatus(ctx context.Context, member *pdpb.Member) *DrAutoSyncStatus
	ReplicateFileToMember(ctx context.Context, member *pdpb.Member, name string, data []byte) error
	GetMembers(context.Context, *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error)
}

const drStatusFile = "DR_STATE"
const minPersistFileTimeout = time.Second * 3

// ModeManager is used to control how raft logs are synchronized between
// different tikv nodes.
type ModeManager struct {
	sync.RWMutex
	config         config.ReplicationModeConfig
	storage        *core.Storage
	cluster        opt.Cluster
	fileReplicater FileReplicater

	drAutoSync DrAutoSyncStatus
	// intermediate states of the recovery process
	// they are accessed without locks as they are only used by background job.
	drRecoverKey   []byte // all regions that has startKey < drRecoverKey are successfully recovered
	drRecoverCount int    // number of regions that has startKey < drRecoverKey
	// When find a region that is not recovered, PD will not check all the
	// remaining regions, but read a region to estimate the overall progress
	drSampleRecoverCount int // number of regions that are recovered in sample
	drSampleTotalRegion  int // number of regions in sample
	drTotalRegion        int // number of all regions

	drMemberSyncStatusTime map[uint64]time.Time
}

// NewReplicationModeManager creates the replicate mode manager.
func NewReplicationModeManager(config config.ReplicationModeConfig, storage *core.Storage, fileReplicater FileReplicater) *ModeManager {
	m := &ModeManager{
		config:         config,
		storage:        storage,
		fileReplicater: fileReplicater,
	}
	return m
}

// Init loads drAutoSyncState to initialize the replicate mode manager
func (m *ModeManager) Init(cluster opt.Cluster) error {
	m.cluster = cluster
	switch m.config.ReplicationMode {
	case modeMajority:
	case modeDRAutoSync:
		if err := m.loadDRAutoSync(); err != nil {
			return err
		}
	}
	return nil
}

// UpdateConfig updates configuration online and updates internal state.
func (m *ModeManager) UpdateConfig(config config.ReplicationModeConfig) error {
	m.Lock()
	defer m.Unlock()
	// If mode change from 'majority' to 'dr-auto-sync', switch to 'sync_recover'.
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
	// If the label key is updated, switch to 'async' state.
	if m.config.ReplicationMode == modeDRAutoSync && config.ReplicationMode == modeDRAutoSync && m.config.DRAutoSync.LabelKey != config.DRAutoSync.LabelKey {
		old := m.config
		m.config = config
		err := m.drSwitchToAsyncWithLock()
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
			LabelKey:            m.config.DRAutoSync.LabelKey,
			State:               pb.DRAutoSyncState(pb.DRAutoSyncState_value[strings.ToUpper(m.drAutoSync.State)]),
			StateId:             m.drAutoSync.StateID,
			WaitSyncTimeoutHint: int32(m.config.DRAutoSync.WaitSyncTimeout.Seconds()),
		}
	}
	return p
}

// UpdateSyncStatusTime ...
func (m *ModeManager) UpdateSyncStatusTime(memberID uint64) *DrAutoSyncStatus {
	m.Lock()
	defer m.Unlock()
	m.drMemberSyncStatusTime[memberID] = typeutil.MonotonicRawClock.Now().Add(m.config.DRAutoSync.WaitSyncTimeout.Duration)
	return &m.drAutoSync
}

// CheckDRAutoSyncLoop ...
func (m *ModeManager) CheckDRAutoSyncLoop(ctx context.Context, member *member.Member) {
	tickTime := m.config.DRAutoSync.WaitSyncTimeout.Duration / 5
	timer := time.NewTicker(tickTime)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			m.Lock()
			defer m.Unlock()
			mode := m.config.ReplicationMode
			switch mode {
			case modeMajority:
			case modeDRAutoSync:
				if member.IsLeader() || m.fileReplicater == nil {
					continue
				}
				leader := member.GetLeader()
				sendTime := typeutil.MonotonicRawClock.Now()
				reqCtx, cancel := context.WithTimeout(ctx, tickTime)
				status := m.fileReplicater.GetDrAutoSyncStatus(reqCtx, leader)
				cancel()
				if status != nil {
					status.ExpireTime = sendTime.Add(m.config.DRAutoSync.WaitSyncTimeout.Duration)
					if err := m.storage.SaveReplicationStatus(modeDRAutoSync, status); err != nil {
						log.Warn("failed to save replication status", zap.Error(err))
					}
					m.drAutoSync = *status
				}
			}
		}
	}
}

// HTTPReplicationStatus is for query status from HTTP API.
type HTTPReplicationStatus struct {
	Mode       string `json:"mode"`
	DrAutoSync struct {
		LabelKey        string    `json:"label_key"`
		State           string    `json:"state"`
		StateID         uint64    `json:"state_id,omitempty"`
		TotalRegions    int       `json:"total_regions,omitempty"`
		SyncedRegions   int       `json:"synced_regions,omitempty"`
		RecoverProgress float32   `json:"recover_progress,omitempty"`
		ExpireTime      time.Time `json:"expire_time,omitempty"`
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
		status.DrAutoSync.ExpireTime = m.drAutoSync.ExpireTime
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
	drStateAsync       = "async"
	drStateSyncRecover = "sync_recover"
)

// DrAutoSyncStatus ...
type DrAutoSyncStatus struct {
	State            string    `json:"state,omitempty"`
	StateID          uint64    `json:"state_id,omitempty"`
	RecoverStartTime time.Time `json:"recover_start,omitempty"`
	TotalRegions     int       `json:"total_regions,omitempty"`
	SyncedRegions    int       `json:"synced_regions,omitempty"`
	RecoverProgress  float32   `json:"recover_progress,omitempty"`
	ExpireTime       time.Time `json:"status_timeout,omitempty"`
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

func (m *ModeManager) drSwitchToAsync() error {
	m.Lock()
	defer m.Unlock()
	return m.drSwitchToAsyncWithLock()
}

func (m *ModeManager) drSwitchToAsyncWithLock() error {
	id, err := m.cluster.AllocID()
	if err != nil {
		log.Warn("failed to switch to async state", zap.String("replicate-mode", modeDRAutoSync), zap.Error(err))
		return err
	}
	dr := DrAutoSyncStatus{State: drStateAsync, StateID: id}
	if err := m.drPersistStatus(dr); err != nil {
		return err
	}
	if err := m.storage.SaveReplicationStatus(modeDRAutoSync, dr); err != nil {
		log.Warn("failed to switch to async state", zap.String("replicate-mode", modeDRAutoSync), zap.Error(err))
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
		log.Warn("failed to switch to sync_recover state", zap.String("replicate-mode", modeDRAutoSync), zap.Error(err))
		return err
	}
	dr := DrAutoSyncStatus{State: drStateSyncRecover, StateID: id, RecoverStartTime: time.Now()}
	if err := m.drPersistStatus(dr); err != nil {
		return err
	}
	if err = m.storage.SaveReplicationStatus(modeDRAutoSync, dr); err != nil {
		log.Warn("failed to switch to sync_recover state", zap.String("replicate-mode", modeDRAutoSync), zap.Error(err))
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
		log.Warn("failed to switch to sync state", zap.String("replicate-mode", modeDRAutoSync), zap.Error(err))
		return err
	}
	dr := DrAutoSyncStatus{State: drStateSync, StateID: id}
	if err := m.drPersistStatus(dr); err != nil {
		return err
	}
	if err := m.storage.SaveReplicationStatus(modeDRAutoSync, dr); err != nil {
		log.Warn("failed to switch to sync state", zap.String("replicate-mode", modeDRAutoSync), zap.Error(err))
		return err
	}
	m.drAutoSync = dr
	log.Info("switched to sync state", zap.String("replicate-mode", modeDRAutoSync))
	return nil
}

func (m *ModeManager) drPersistStatus(status DrAutoSyncStatus) error {
	if m.fileReplicater != nil {
		ctx, cancel := context.WithTimeout(context.Background(), m.config.DRAutoSync.WaitSyncTimeout.Duration)
		resp, err := m.fileReplicater.GetMembers(ctx, nil)
		cancel()
		if err != nil {
			return err
		}
		data, _ := json.Marshal(status)
		var wg sync.WaitGroup
		for _, member := range resp.Members {
			if member.MemberId == resp.Leader.MemberId {
				continue
			}
			wg.Add(1)
			go func(member *pdpb.Member) {
				defer wg.Done()
				expireTime, ok := m.drMemberSyncStatusTime[member.MemberId]
				var timeout time.Duration
				if status.State == drStateAsync && (!ok || expireTime.After(typeutil.MonotonicRawClock.Now())) {
					timeout = m.config.DRAutoSync.WaitSyncTimeout.Duration
				} else {
					timeout = minPersistFileTimeout
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				if err := m.fileReplicater.ReplicateFileToMember(ctx, member, drStatusFile, data); err != nil {
					log.Warn("failed to replicate state",
						zap.String("replicate-mode", modeDRAutoSync),
						zap.String("new-state", status.State),
						zap.String("member", member.Name),
						zap.Error(err))
				}
			}(member)
			wg.Wait()
		}
	}
	return nil
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
func (m *ModeManager) Run(quit chan struct{}) {
	// Wait for a while when just start, in case tikv do not connect in time.
	select {
	case <-time.After(idleTimeout):
	case <-quit:
		return
	}
	for {
		select {
		case <-time.After(tickInterval):
		case <-quit:
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

	totalPrimary, totalDr := m.config.DRAutoSync.PrimaryReplicas, m.config.DRAutoSync.DRReplicas
	downPrimary, downDr := m.checkStoreStatus()

	// canSync is true when every region has at least 1 replica in each DC.
	canSync := downPrimary < totalPrimary && downDr < totalDr

	// hasMajority is true when every region has majority peer online.
	var upPeers int
	if downPrimary < totalPrimary {
		upPeers += totalPrimary - downPrimary
	}
	if downDr < totalDr {
		upPeers += totalDr - downDr
	}
	hasMajority := upPeers*2 > totalPrimary+totalDr

	// If hasMajority is false, the cluster is always unavailable. Switch to async won't help.
	if !canSync && hasMajority && m.drGetState() != drStateAsync {
		m.drSwitchToAsync()
	}

	if canSync && m.drGetState() == drStateAsync {
		m.drSwitchToSyncRecover()
	}

	if m.drGetState() == drStateSyncRecover {
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

func (m *ModeManager) checkStoreStatus() (primaryFailCount, drFailCount int) {
	m.RLock()
	defer m.RUnlock()
	for _, s := range m.cluster.GetStores() {
		if !s.IsTombstone() && s.DownTime() >= m.config.DRAutoSync.WaitStoreTimeout.Duration {
			labelValue := s.GetLabelValue(m.config.DRAutoSync.LabelKey)
			if labelValue == m.config.DRAutoSync.Primary {
				primaryFailCount++
			}
			if labelValue == m.config.DRAutoSync.DR {
				drFailCount++
			}
		}
	}
	return
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
			log.Warn("scan empty regions", zap.ByteString("recover-key", m.drRecoverKey))
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
	uncheckRecoverd := float32(totalUnchecked) * float32(m.drSampleRecoverCount) / float32(m.drSampleTotalRegion)
	return (float32(m.drRecoverCount) + uncheckRecoverd) / float32(total)
}

func (m *ModeManager) checkRegionRecover(region *core.RegionInfo, startKey []byte) bool {
	if !bytes.Equal(startKey, region.GetStartKey()) {
		log.Warn("found region gap", zap.ByteString("key", startKey), zap.ByteString("region-start-key", region.GetStartKey()), zap.Uint64("region-id", region.GetID()))
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
