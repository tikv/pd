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

package dashboard

import (
	"context"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/pingcap-incubator/tidb-dashboard/pkg/apiserver"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"go.uber.org/zap"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/logutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/cluster"
)

const (
	checkInterval = time.Second
)

// Manager is used to control dashboard.
type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	srv        *server.Server
	service    *apiserver.Service
	redirector *Redirector

	enableDynamic bool

	isLeader bool
	rc       *cluster.RaftCluster
	members  []*pdpb.Member
}

// NewManager creates a new Manager.
func NewManager(srv *server.Server, s *apiserver.Service, redirector *Redirector) *Manager {
	ctx, cancel := context.WithCancel(srv.Context())
	return &Manager{
		ctx:           ctx,
		cancel:        cancel,
		srv:           srv,
		service:       s,
		redirector:    redirector,
		enableDynamic: srv.GetConfig().EnableDynamicConfig,
	}
}

func (m *Manager) start() {
	m.wg.Add(1)
	go m.serviceLoop()
}

func (m *Manager) stop() {
	m.cancel()
	m.wg.Wait()
	log.Info("exit dashboard loop")
}

func (m *Manager) serviceLoop() {
	defer logutil.LogPanic()
	defer m.wg.Done()

	ticker := time.NewTicker(checkInterval)
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
		m.rc = nil
		m.members = nil
		if !m.enableDynamic {
			m.srv.GetScheduleOption().Reload(m.srv.GetStorage())
		}
		return
	}

	m.isLeader = true
	m.rc = m.srv.GetRaftCluster()
	if m.rc == nil || !m.rc.IsRunning() {
		m.members = nil
		return
	}

	var err error
	if m.members, err = cluster.GetMembers(m.srv.GetClient()); err != nil {
		log.Error("failed to get members")
	}
}

// checkDashboardAddress checks if the dashboard service needs to change due to dashboard address is changed.
func (m *Manager) checkAddress() {
	dashboardAddress := m.srv.GetScheduleOption().GetDashboardAddress()
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
		if _, err := url.Parse(dashboardAddress); err != nil {
			log.Error("illegal dashboard address", zap.String("address", dashboardAddress))
			return
		}

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
	// get new dashboard address
	members := m.members
	var addr string
	switch len(members) {
	case 1:
		addr = members[0].GetClientUrls()[0]
	default:
		addr = members[0].GetClientUrls()[0]
		leaderID := m.srv.GetMemberInfo().MemberId
		sort.Slice(members, func(i, j int) bool { return members[i].GetMemberId() < members[j].GetMemberId() })
		for _, member := range members {
			if member.MemberId != leaderID {
				addr = member.GetClientUrls()[0]
				break
			}
		}
	}
	// set new dashboard address
	if m.enableDynamic {
		if err := m.srv.UpdateConfigManager("pd-server.dashboard-address", addr); err != nil {
			log.Error("failed to update the dashboard address in config manager", zap.Error(err))
		}
		return
	}
	cfg := m.srv.GetScheduleOption().GetPDServerConfig().Clone()
	cfg.DashboardAddress = addr
	m.srv.SetPDServerConfig(*cfg)
}

func (m *Manager) startService() {
	if m.service.IsRunning() {
		return
	}
	if err := m.service.Start(m.ctx); err != nil {
		log.Error("Can not start dashboard server", zap.Error(err))
	} else {
		log.Info("Dashboard server is started", zap.String("path", uiServiceGroup.PathPrefix))
	}
}

func (m *Manager) stopService() {
	if !m.service.IsRunning() {
		return
	}
	if err := m.service.Stop(context.Background()); err != nil {
		log.Error("Stop dashboard server error", zap.Error(err))
	} else {
		log.Info("Dashboard server is stopped")
	}
}
