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

// CheckInterval is the interval to check dashboard address.
var CheckInterval = time.Minute

// Manager is used to control dashboard.
type Manager struct {
	autoSetAddress bool

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
		autoSetAddress: srv.GetConfig().EnableDynamicConfig,
		ctx:            ctx,
		cancel:         cancel,
		srv:            srv,
		service:        s,
		redirector:     redirector,
	}
}

func (m *Manager) start() {
	m.wg.Add(1)
	go m.serviceLoop()
}

func (m *Manager) serviceLoop() {
	defer logutil.LogPanic()
	defer m.wg.Done()

	ticker := time.NewTicker(CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.checkAddress()
		case <-m.ctx.Done():
			m.stopService()
			log.Info("dashboard is closed, exit dashboard loop")
			return
		}
	}
}

func (m *Manager) stop() {
	m.cancel()
	m.wg.Wait()
}

// checkDashboardAddress checks if the dashboard service needs to change due to dashboard address is changed.
func (m *Manager) checkAddress() {
	dashboardAddress := m.srv.GetScheduleOption().GetDashboardAddress()
	switch dashboardAddress {
	case "auto":
		if m.autoSetAddress && m.srv.GetMember().IsLeader() {
			rc := m.srv.GetRaftCluster()
			if rc == nil || !rc.IsRunning() {
				return
			}
			members, err := cluster.GetMembers(m.srv.GetClient())
			if err != nil || len(members) == 0 {
				log.Error("failed to get members")
				return
			}
			sort.Slice(members, func(i, j int) bool { return members[i].GetMemberId() < members[j].GetMemberId() })
			for i := range members {
				if len(members[i].GetClientUrls()) != 0 {
					addr := members[i].GetClientUrls()[0]
					if err := m.srv.UpdateConfigManager("pd-server.dashboard-address", addr); err != nil {
						log.Error("failed to update the dashboard address in config manager", zap.Error(err))
					} else {
						rc.SetDashboardAddress(addr)
					}
					break
				}
			}
		}
		return
	case "none":
		m.redirector.SetAddress("")
		m.stopService()
		return
	default:
		_, err := url.Parse(dashboardAddress)
		if err != nil {
			log.Error("illegal dashboard address", zap.String("address", dashboardAddress))
			return
		}
	}

	m.redirector.SetAddress(dashboardAddress)

	var addr string
	if len(m.srv.GetMemberInfo().GetClientUrls()) != 0 {
		addr = m.srv.GetMemberInfo().GetClientUrls()[0]
	}
	if addr == dashboardAddress {
		m.startService()
	} else {
		m.stopService()
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
