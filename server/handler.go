// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import "github.com/juju/errors"

var (
	errNotBootstrapped = errors.New("TiKV cluster not bootstrapped")
)

// Handler is a helper to export methods to handle API/RPC requests.
type Handler struct {
	s *Server
}

func newHandler(s *Server) *Handler {
	return &Handler{s: s}
}

func (h *Handler) getCoordinator() (*coordinator, error) {
	cluster := h.s.GetRaftCluster()
	if cluster == nil {
		return nil, errors.Trace(errNotBootstrapped)
	}
	return cluster.coordinator, nil
}

// GetSchedulers returns all names of schedulers.
func (h *Handler) GetSchedulers() ([]string, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c.getSchedulers(), nil
}

// RemoveScheduler removes a scheduler by name.
func (h *Handler) RemoveScheduler(name string) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}
	if !c.removeScheduler(name) {
		return errors.Errorf("scheduler %q not found", name)
	}
	return nil
}

// AddLeaderScheduler adds a leader scheduler.
func (h *Handler) AddLeaderScheduler(s Scheduler) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}
	if !c.addScheduler(newLeaderScheduleController(c, s)) {
		return errors.Errorf("scheduler %q exists", s.GetName())
	}
	return nil
}

// AddLeaderBalancer adds a leader-balancer.
func (h *Handler) AddLeaderBalancer() error {
	return h.AddLeaderScheduler(newLeaderBalancer(h.s.scheduleOpt))
}

// AddGrantLeaderScheduler adds a grant-leader-scheduler.
func (h *Handler) AddGrantLeaderScheduler(storeID uint64) error {
	return h.AddLeaderScheduler(newGrantLeaderScheduler(storeID))
}
