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

package adapter

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-dashboard/pkg/apiserver"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
)

var (
	// CheckInterval represents the time interval of running check.
	CheckInterval = time.Second
)

// Manager is used to control dashboard.
type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	srv        *server.Server
	service    *apiserver.Service
	redirector *Redirector

	isLeader bool
	members  []*pdpb.Member
}

// NewManager creates a new Manager.
func NewManager(srv *server.Server, s *apiserver.Service, redirector *Redirector) *Manager {
	return &Manager{
		srv:        srv,
		service:    s,
		redirector: redirector,
	}
}

// Start monitoring the dynamic config and control the dashboard.
func (m *Manager) Start() {
	m.ctx, m.cancel = context.WithCancel(m.srv.Context())
	m.wg.Add(1)
	m.isLeader = false
	m.members = nil
	go m.serviceLoop()
}

// Stop monitoring the dynamic config and control the dashboard.
func (m *Manager) Stop() {
	m.cancel()
	m.wg.Wait()
	log.Info("exit dashboard loop")
}

func (m *Manager) serviceLoop() {
	defer logutil.LogPanic()
	defer m.wg.Done()
	// TODO: After we fix the atomic problem of config, we can remove this failpoint.
	failpoint.Inject("skipDashboardLoop", func() {
		failpoint.Return()
	})

	ticker := time.NewTicker(CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			m.stopService()
			return
		case <-ticker.C:
			m.updateInfo()
			m.checkAddress()
		}
	}
}

// updateInfo updates information from the server.
func (m *Manager) updateInfo() {
	if !m.srv.GetMember().IsLeader() {
		m.isLeader = false
		m.members = nil
		if err := m.srv.GetPersistOptions().Reload(m.srv.GetStorage()); err != nil {
			log.Warn("failed to reload persist options")
		}
		return
	}

	m.isLeader = true

	var err error
	if m.members, err = cluster.GetMembers(m.srv.GetClient()); err != nil {
		log.Warn("failed to get members", errs.ZapError(err))
		m.members = nil
		return
	}

	for _, member := range m.members {
		if len(member.GetClientUrls()) == 0 {
			log.Warn("failed to get member client urls")
			m.members = nil
			return
		}
	}
}

// checkDashboardAddress checks if the dashboard service needs to change due to dashboard address is changed.
func (m *Manager) checkAddress() {
	dashboardAddress := m.srv.GetPersistOptions().GetDashboardAddress()
	switch dashboardAddress {
	case "auto":
		if m.isLeader && len(m.members) > 0 {
			m.setNewAddress()
		}
		return
	case "none":
		m.redirector.SetAddress("")
		m.stopService()
		return
	default:
		if m.isLeader && m.needResetAddress(dashboardAddress) {
			m.setNewAddress()
			return
		}
	}

	m.redirector.SetAddress(dashboardAddress)

	clientUrls := m.srv.GetMemberInfo().GetClientUrls()
	if len(clientUrls) > 0 && clientUrls[0] == dashboardAddress {
		m.startService()
	} else {
		m.stopService()
	}
}

func (m *Manager) needResetAddress(addr string) bool {
	if len(m.members) == 0 {
		return false
	}

	for _, member := range m.members {
		if member.GetClientUrls()[0] == addr {
			return false
		}
	}

	return true
}

func (m *Manager) setNewAddress() {
	// select the sever with minimum member ID(avoid the PD leader if possible) to run dashboard.
	minMemberIdx := 0
	if len(m.members) > 1 {
		leaderID := m.srv.GetMemberInfo().GetMemberId()
		for idx, member := range m.members {
			curMemberID := member.GetMemberId()
			if curMemberID != leaderID && curMemberID < m.members[minMemberIdx].GetMemberId() {
				minMemberIdx = idx
				break
			}
		}
	}

	// set new dashboard address
	cfg := m.srv.GetPersistOptions().GetPDServerConfig().Clone()
	cfg.DashboardAddress = m.members[minMemberIdx].GetClientUrls()[0]
	if err := m.srv.SetPDServerConfig(*cfg); err != nil {
		log.Warn("failed to set persist options")
	}
}

func (m *Manager) startService() {
	if m.service.IsRunning() {
		return
	}
	if err := m.service.Start(m.ctx); err != nil {
		log.Error("can not start dashboard server", errs.ZapError(errs.ErrDashboardStart, err))
	} else {
		log.Info("dashboard server is started")
	}
}

func (m *Manager) stopService() {
	if !m.service.IsRunning() {
		return
	}
	if err := m.service.Stop(context.Background()); err != nil {
		log.Error("stop dashboard server error", errs.ZapError(errs.ErrDashboardStop, err))
	} else {
		log.Info("dashboard server is stopped")
	}
}
