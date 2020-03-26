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

package replicate

import (
	"strings"
	"sync"

	pb "github.com/pingcap/kvproto/pkg/replicate_mode"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/id"
)

const (
	modeMajority   = "majority"
	modeDRAutosync = "dr_autosync"
)

// ModeManager is used to control how raft logs are synchronized between
// different tikv nodes.
type ModeManager struct {
	sync.RWMutex
	config  config.ReplicateModeConfig
	storage *core.Storage
	idAlloc id.Allocator

	drAutosync drAutosyncStatus
}

// NewReplicateModeManager creates the replicate mode manager.
func NewReplicateModeManager(config config.ReplicateModeConfig, storage *core.Storage, idAlloc id.Allocator) (*ModeManager, error) {
	m := &ModeManager{
		config:  config,
		storage: storage,
		idAlloc: idAlloc,
	}
	switch config.ReplicateMode {
	case modeMajority:
	case modeDRAutosync:
		if err := m.loadDRAutosync(); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// GetReplicateStatus returns the status to sync with tikv servers.
func (m *ModeManager) GetReplicateStatus() *pb.ReplicateStatus {
	m.RLock()
	defer m.RUnlock()

	p := &pb.ReplicateStatus{
		Mode: pb.ReplicateStatus_Mode(pb.ReplicateStatus_Mode_value[strings.ToUpper(m.config.ReplicateMode)]),
	}
	switch m.config.ReplicateMode {
	case modeMajority:
	case modeDRAutosync:
		p.DrAutosync = &pb.DRAutoSync{
			LabelKey: m.config.DRAutoSync.LabelKey,
			State:    pb.DRAutoSync_State(pb.DRAutoSync_State_value[strings.ToUpper(m.drAutosync.State)]),
		}
		if m.drAutosync.State == drStateSyncRecover {
			p.DrAutosync.RecoverId = m.drAutosync.RecoverID
			p.DrAutosync.WaitSyncTimeoutHint = int32(m.config.DRAutoSync.WaitSyncTimeout.Seconds())
		}
	}
	return p
}

const (
	drStateSync        = "sync"
	drStateAsync       = "async"
	drStateSyncRecover = "sync_recover"
)

type drAutosyncStatus struct {
	State     string `json:"state,omitempty"`
	RecoverID uint64 `json:"recover_id,omitempty"`
}

func (m *ModeManager) loadDRAutosync() error {
	ok, err := m.storage.LoadReplicateStatus(modeDRAutosync, &m.drAutosync)
	if err != nil {
		return err
	}
	if !ok {
		// initialize
		m.drAutosync = drAutosyncStatus{State: drStateSync}
	}
	return nil
}

func (m *ModeManager) drSwitchToAsync() error {
	m.Lock()
	defer m.Unlock()
	dr := drAutosyncStatus{State: drStateAsync}
	if err := m.storage.SaveReplicateStatus(modeDRAutosync, dr); err != nil {
		return err
	}
	m.drAutosync = dr
	return nil
}

func (m *ModeManager) drSwitchToSyncRecover() error {
	m.Lock()
	defer m.Unlock()
	id, err := m.idAlloc.Alloc()
	if err != nil {
		return err
	}
	dr := drAutosyncStatus{State: drStateSyncRecover, RecoverID: id}
	if err = m.storage.SaveReplicateStatus(modeDRAutosync, dr); err != nil {
		return err
	}
	m.drAutosync = dr
	return nil
}

func (m *ModeManager) drSwitchToSync() error {
	m.Lock()
	defer m.Unlock()
	dr := drAutosyncStatus{State: drStateSync}
	if err := m.storage.SaveReplicateStatus(modeDRAutosync, dr); err != nil {
		return err
	}
	m.drAutosync = dr
	return nil
}
