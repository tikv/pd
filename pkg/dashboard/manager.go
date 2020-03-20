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
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/logutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/cluster"
	"go.uber.org/zap"
)

// CheckInterval is the interval to check dashbaord address.
const CheckInterval = time.Minute

// Manager is used to control dashboard.
type Manager struct {
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	s                *server.Server
	service          *apiserver.Service
	isDashboardServe bool
}

// NewManager creates a new Manager.
func NewManager(s *server.Server, service *apiserver.Service) *Manager {
	ctx, cancel := context.WithCancel(s.Context())
	return &Manager{
		ctx:     ctx,
		cancel:  cancel,
		s:       s,
		service: service,
	}
}

func (m *Manager) start() {
	m.wg.Add(1)
	go m.dashboardLoop()
}

func (m *Manager) dashboardLoop() {
	defer logutil.LogPanic()
	defer m.wg.Done()

	dashboardTicker := time.NewTicker(CheckInterval)
	defer dashboardTicker.Stop()
	for {
		select {
		case <-dashboardTicker.C:
			m.checkDashboardAddress()
		case <-m.ctx.Done():
			if m.isDashboardServe {
				m.stopDashboard()
			}
			log.Info("server is closed, exit dashboard loop")
			return
		}
	}
}

func (m *Manager) stop() {
	m.cancel()
	m.wg.Wait()
}

// checkDashboardAddress checks if the dashboard service needs to change due to dashboard address is changed.
func (m *Manager) checkDashboardAddress() {
	dashboardAddress := m.s.GetScheduleOption().GetDashboardAddress()
	switch dashboardAddress {
	case "auto":
		if m.s.GetMember().IsLeader() {
			rc := m.s.GetRaftCluster()
			if rc == nil || !rc.IsRunning() {
				return
			}
			members, err := cluster.GetMembers(m.s.GetClient())
			if err != nil || len(members) == 0 {
				log.Error("failed to get members")
			}
			sort.Slice(members, func(i, j int) bool { return members[i].GetMemberId() < members[j].GetMemberId() })
			for i := range members {
				if len(members[i].GetClientUrls()) != 0 {
					addr := members[i].GetClientUrls()[0]
					if m.s.GetConfig().EnableDynamicConfig {
						err := m.s.UpdateConfigManager("pd-server.dashboard-address", addr)
						if err != nil {
							log.Error("failed to update the dashboard address in config manager", zap.Error(err))
						}
					}
					rc.SetDashboardAddress(addr)
					break
				}
			}
		}
		return
	case "none":
		if m.isDashboardServe {
			m.stopDashboard()
			m.isDashboardServe = false
		}
		return
	default:
	}
	_, err := url.Parse(dashboardAddress)
	if err != nil {
		log.Error("illegal dashboard address", zap.String("address", dashboardAddress))
		return
	}

	var addr string
	if len(m.s.GetMemberInfo().GetClientUrls()) != 0 {
		addr = m.s.GetMemberInfo().GetClientUrls()[0]
	}
	if addr != dashboardAddress && m.isDashboardServe {
		m.stopDashboard()
		m.isDashboardServe = false
	}

	if addr == dashboardAddress && !m.isDashboardServe {
		m.startDashboard()
		m.isDashboardServe = true
	}
}

func (m *Manager) startDashboard() {
	if err := m.service.Start(m.ctx); err != nil {
		log.Error("Can not start dashboard server", zap.Error(err))
	} else {
		log.Info("Dashboard server is started", zap.String("path", uiServiceGroup.PathPrefix))
	}
}

func (m *Manager) stopDashboard() {
	if err := m.service.Stop(context.Background()); err != nil {
		log.Error("Stop dashboard server error", zap.Error(err))
	} else {
		log.Info("Dashboard server is stopped")
	}
}
