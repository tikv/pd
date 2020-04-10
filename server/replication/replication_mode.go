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
	"strings"
	"sync"
	"time"

	pb "github.com/pingcap/kvproto/pkg/replication_modepb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"go.uber.org/zap"
)

const (
	modeMajority   = "majority"
	modeDRAutoSync = "dr_auto_sync"
)

// ModeManager is used to control how raft logs are synchronized between
// different tikv nodes.
type ModeManager struct {
	sync.RWMutex
	config  config.ReplicationModeConfig
	storage *core.Storage
	cluster opt.Cluster

	drAutoSync drAutoSyncStatus
}

// NewReplicationModeManager creates the replicate mode manager.
func NewReplicationModeManager(config config.ReplicationModeConfig, storage *core.Storage, cluster opt.Cluster) (*ModeManager, error) {
	m := &ModeManager{
		config:  config,
		storage: storage,
		cluster: cluster,
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

// GetReplicationStatus returns the status to sync with tikv servers.
func (m *ModeManager) GetReplicationStatus() *pb.ReplicationStatus {
	m.RLock()
	defer m.RUnlock()

	p := &pb.ReplicationStatus{
		Mode: pb.ReplicationMode(pb.ReplicationMode_value[strings.ToUpper(m.config.ReplicationMode)]),
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

// HTTPReplicationStatus is for query status from HTTP API.
type HTTPReplicationStatus struct {
	Mode       string `json:"mode"`
	DrAutoSync struct {
		LabelKey        string  `json:"label_key"`
		State           string  `json:"state"`
		StateID         uint64  `json:"state_id,omitempty"`
		RecoverProgress float32 `json:"recover_progress,omitempty"`
	} `json:"dr_auto_sync,omitempty"`
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

type drAutoSyncStatus struct {
	State            string    `json:"state,omitempty"`
	StateID          uint64    `json:"state_id,omitempty"`
	RecoverStartTime time.Time `json:"recover_start,omitempty"`
	RecoverProgress  float32   `json:"recover_progress,omitempty"`
}

func (m *ModeManager) loadDRAutoSync() error {
	ok, err := m.storage.LoadReplicationStatus(modeDRAutoSync, &m.drAutoSync)
	if err != nil {
		return err
	}
	if !ok {
		// initialize
		id, err := m.cluster.AllocID()
		if err != nil {
			return err
		}
		m.drAutoSync = drAutoSyncStatus{State: drStateSync, StateID: id}
	}
	return nil
}

func (m *ModeManager) drSwitchToAsync() error {
	m.Lock()
	defer m.Unlock()
	id, err := m.cluster.AllocID()
	if err != nil {
		log.Warn("failed to switch to async state", zap.String("replicate-mode", modeDRAutoSync), zap.Error(err))
		return err
	}
	dr := drAutoSyncStatus{State: drStateAsync, StateID: id}
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
	id, err := m.cluster.AllocID()
	if err != nil {
		log.Warn("failed to switch to sync_recover state", zap.String("replicate-mode", modeDRAutoSync), zap.Error(err))
		return err
	}
	dr := drAutoSyncStatus{State: drStateSyncRecover, StateID: id, RecoverStartTime: time.Now()}
	if err = m.storage.SaveReplicationStatus(modeDRAutoSync, dr); err != nil {
		log.Warn("failed to switch to sync_recover state", zap.String("replicate-mode", modeDRAutoSync), zap.Error(err))
		return err
	}
	m.drAutoSync = dr
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
	dr := drAutoSyncStatus{State: drStateSync, StateID: id}
	if err := m.storage.SaveReplicationStatus(modeDRAutoSync, dr); err != nil {
		log.Warn("failed to switch to sync state", zap.String("replicate-mode", modeDRAutoSync), zap.Error(err))
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

	canSync := m.checkCanSync()

	if !canSync && m.drGetState() != drStateAsync {
		m.drSwitchToAsync()
	}

	if canSync && m.drGetState() == drStateAsync {
		m.drSwitchToSyncRecover()
	}

	if m.drGetState() == drStateSyncRecover {
		current, total := m.recoverProgress()
		if current >= total {
			m.drSwitchToSync()
		} else {
			m.updateRecoverProgress(float32(current) / float32(total))
		}
	}
}

func (m *ModeManager) checkCanSync() bool {
	m.RLock()
	defer m.RUnlock()
	var countPrimary, countDR int
	for _, s := range m.cluster.GetStores() {
		if !s.IsTombstone() && s.DownTime() >= m.config.DRAutoSync.WaitStoreTimeout.Duration {
			labelValue := s.GetLabelValue(m.config.DRAutoSync.LabelKey)
			if labelValue == m.config.DRAutoSync.Primary {
				countPrimary++
			}
			if labelValue == m.config.DRAutoSync.DR {
				countDR++
			}
		}
	}
	return countPrimary < m.config.DRAutoSync.PrimaryReplicas && countDR < m.config.DRAutoSync.DRReplicas
}

func (m *ModeManager) recoverProgress() (current, total int) {
	m.RLock()
	defer m.RUnlock()
	var key []byte
	for len(key) > 0 || total == 0 {
		regions := m.cluster.ScanRegions(key, nil, 1024)
		if len(regions) == 0 {
			log.Warn("scan empty regions", zap.ByteString("start-key", key))
			total++ // make sure it won't complete
			return
		}

		total += len(regions)
		for _, r := range regions {
			if !bytes.Equal(key, r.GetStartKey()) {
				log.Warn("found region gap", zap.ByteString("key", key), zap.ByteString("region-start-key", r.GetStartKey()), zap.Uint64("region-id", r.GetID()))
				total++
			}
			if r.GetReplicationStatus().GetStateId() == m.drAutoSync.StateID &&
				r.GetReplicationStatus().GetState() == pb.RegionReplicationState_INTEGRITY_OVER_LABEL {
				current++
			}
			key = r.GetEndKey()
		}
	}
	return
}

func (m *ModeManager) updateRecoverProgress(progress float32) {
	m.Lock()
	defer m.Unlock()
	m.drAutoSync.RecoverProgress = progress
}
