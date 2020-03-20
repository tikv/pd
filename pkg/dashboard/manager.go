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
	"go.uber.org/zap"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/logutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/cluster"
)

type electionStatus int

const (
	stopped electionStatus = iota
	unknown
	known

	fastCheckInterval = time.Second
	slowCheckInterval = time.Minute
)

// Manager is used to control dashboard.
type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	srv        *server.Server
	service    *apiserver.Service
	redirector *Redirector
}

// NewManager creates a new Manager.
func NewManager(srv *server.Server, s *apiserver.Service, redirector *Redirector) *Manager {
	ctx, cancel := context.WithCancel(srv.Context())
	return &Manager{
		ctx:        ctx,
		cancel:     cancel,
		srv:        srv,
		service:    s,
		redirector: redirector,
	}
}

func (m *Manager) start() {
	m.wg.Add(1)
	go func() {
		defer logutil.LogPanic()
		defer m.wg.Done()

		status := unknown
		for status != stopped {
			status = m.serviceLoop(status)
		}
	}()
}

func (m *Manager) stop() {
	m.cancel()
	m.wg.Wait()
	log.Info("exit dashboard loop")
}

func (m *Manager) serviceLoop(lastStatus electionStatus) (nextStatus electionStatus) {
	var ticker *time.Ticker
	switch lastStatus {
	case unknown:
		ticker = time.NewTicker(fastCheckInterval)
	case known:
		ticker = time.NewTicker(slowCheckInterval)
	default:
		panic("unreachable")
	}
	defer ticker.Stop()

	nextStatus = lastStatus
	for nextStatus == lastStatus {
		select {
		case <-m.ctx.Done():
			return stopped
		case <-ticker.C:
			nextStatus = m.checkAddress()
		}
	}

	return
}

// checkDashboardAddress checks if the dashboard service needs to change due to dashboard address is changed.
func (m *Manager) checkAddress() electionStatus {
	dashboardAddress := m.srv.GetScheduleOption().GetDashboardAddress()
	switch dashboardAddress {
	case "auto":
		if m.srv.GetMember().IsLeader() {
			m.setNewDashboardAddress()
		}
		return unknown
	case "none":
		m.redirector.SetAddress("")
		m.stopService()
		return known
	default:
		if _, err := url.Parse(dashboardAddress); err != nil {
			log.Error("illegal dashboard address", zap.String("address", dashboardAddress))
			return unknown
		}
		if m.srv.GetMember().IsLeader() && !m.isOnline(dashboardAddress) {
			m.setNewDashboardAddress()
			return unknown
		}
	}

	m.redirector.SetAddress(dashboardAddress)

	clientUrls := m.srv.GetMemberInfo().GetClientUrls()
	if len(clientUrls) > 0 && clientUrls[0] == dashboardAddress {
		m.startService()
	} else {
		m.stopService()
	}

	return known
}

func (m *Manager) isOnline(addr string) bool {
	rc := m.srv.GetRaftCluster()
	if rc == nil || !rc.IsRunning() {
		return true
	}
	members, err := cluster.GetMembers(m.srv.GetClient())
	if err != nil {
		log.Error("failed to get members")
		return true
	}

	for _, member := range members {
		if member.GetClientUrls()[0] == addr {
			return true
		}
	}

	return false
}

func (m *Manager) setNewDashboardAddress() {
	rc := m.srv.GetRaftCluster()
	if rc == nil || !rc.IsRunning() {
		return
	}
	members, err := cluster.GetMembers(m.srv.GetClient())
	if err != nil || len(members) == 0 {
		log.Error("failed to get members")
		return
	}

	// get new dashboard address
	var addr string
	switch len(members) {
	case 0:
		return
	case 1:
		addr = members[0].GetClientUrls()[0]
	case 2:
		addr = members[0].GetClientUrls()[0]
		leaderId := m.srv.GetMemberInfo().MemberId
		sort.Slice(members, func(i, j int) bool { return members[i].GetMemberId() < members[j].GetMemberId() })
		for _, member := range members {
			if member.MemberId != leaderId {
				addr = member.GetClientUrls()[0]
				break
			}
		}
	}

	// set new dashboard address
	rc.SetDashboardAddress(addr)
	if m.srv.GetConfig().EnableDynamicConfig {
		if err := m.srv.UpdateConfigManager("pd-server.dashboard-address", addr); err != nil {
			log.Error("failed to update the dashboard address in config manager", zap.Error(err))
		}
	}
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
