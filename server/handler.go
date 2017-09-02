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

import (
	"github.com/juju/errors"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedulers"
)

var (
	errNotBootstrapped  = errors.New("TiKV cluster not bootstrapped, please start TiKV first")
	errOperatorNotFound = errors.New("operator not found")
)

// Handler is a helper to export methods to handle API/RPC requests.
type Handler struct {
	s   *Server
	opt *scheduleOption
}

func newHandler(s *Server) *Handler {
	return &Handler{s: s, opt: s.scheduleOpt}
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

// GetHotWriteRegions gets all hot regions status
func (h *Handler) GetHotWriteRegions() *StoreHotRegionInfos {
	c, err := h.getCoordinator()
	if err != nil {
		return nil
	}
	return c.getHotWriteRegions()
}

// GetHotWriteStores gets all hot write stores status
func (h *Handler) GetHotWriteStores() map[uint64]uint64 {
	return h.s.cluster.cachedCluster.getStoresWriteStat()
}

// AddScheduler adds a scheduler.
func (h *Handler) AddScheduler(s schedule.Scheduler) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(c.addScheduler(s, minScheduleInterval))
}

// RemoveScheduler removes a scheduler by name.
func (h *Handler) RemoveScheduler(name string) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(c.removeScheduler(name))
}

// AddBalanceLeaderScheduler adds a balance-leader-scheduler.
func (h *Handler) AddBalanceLeaderScheduler() error {
	return h.AddScheduler(schedulers.NewBalanceLeaderScheduler(h.opt))
}

// AddGrantLeaderScheduler adds a grant-leader-scheduler.
func (h *Handler) AddGrantLeaderScheduler(storeID uint64) error {
	return h.AddScheduler(schedulers.NewGrantLeaderScheduler(h.opt, storeID))
}

// AddEvictLeaderScheduler adds an evict-leader-scheduler.
func (h *Handler) AddEvictLeaderScheduler(storeID uint64) error {
	return h.AddScheduler(schedulers.NewEvictLeaderScheduler(h.opt, storeID))
}

// AddShuffleLeaderScheduler adds a shuffle-leader-scheduler.
func (h *Handler) AddShuffleLeaderScheduler() error {
	return h.AddScheduler(schedulers.NewShuffleLeaderScheduler(h.opt))
}

// AddShuffleRegionScheduler adds a shuffle-region-scheduler.
func (h *Handler) AddShuffleRegionScheduler() error {
	return h.AddScheduler(schedulers.NewShuffleRegionScheduler(h.opt))
}

// GetOperator returns the region operator.
func (h *Handler) GetOperator(regionID uint64) (schedule.Operator, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, errors.Trace(err)
	}

	op := c.getOperator(regionID)
	if op == nil {
		return nil, errOperatorNotFound
	}

	return op, nil
}

// RemoveOperator removes the region operator.
func (h *Handler) RemoveOperator(regionID uint64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}

	op := c.getOperator(regionID)
	if op == nil {
		return errOperatorNotFound
	}

	c.removeOperator(op)
	return nil
}

// GetOperators returns the running operators.
func (h *Handler) GetOperators() ([]schedule.Operator, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c.getOperators(), nil
}

// GetAdminOperators returns the running admin operators.
func (h *Handler) GetAdminOperators() ([]schedule.Operator, error) {
	return h.GetOperatorsOfKind(core.AdminKind)
}

// GetLeaderOperators returns the running leader operators.
func (h *Handler) GetLeaderOperators() ([]schedule.Operator, error) {
	return h.GetOperatorsOfKind(core.LeaderKind)
}

// GetRegionOperators returns the running region operators.
func (h *Handler) GetRegionOperators() ([]schedule.Operator, error) {
	return h.GetOperatorsOfKind(core.RegionKind)
}

// GetOperatorsOfKind returns the running operators of the kind.
func (h *Handler) GetOperatorsOfKind(kind core.ResourceKind) ([]schedule.Operator, error) {
	ops, err := h.GetOperators()
	if err != nil {
		return nil, errors.Trace(err)
	}
	var results []schedule.Operator
	for _, op := range ops {
		if op.GetResourceKind() == kind {
			results = append(results, op)
		}
	}
	return results, nil
}

// GetHistoryOperators returns history operators
func (h *Handler) GetHistoryOperators() ([]schedule.Operator, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c.getHistories(), nil
}

// GetHistoryOperatorsOfKind returns history operators by Kind
func (h *Handler) GetHistoryOperatorsOfKind(kind core.ResourceKind) ([]schedule.Operator, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c.getHistoriesOfKind(kind), nil
}

// AddTransferLeaderOperator adds an operator to transfer leader to the store.
func (h *Handler) AddTransferLeaderOperator(regionID uint64, storeID uint64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}

	region := c.cluster.getRegion(regionID)
	if region == nil {
		return errRegionNotFound(regionID)
	}
	newLeader := region.GetStorePeer(storeID)
	if newLeader == nil {
		return errors.Errorf("region has no peer in store %v", storeID)
	}

	op := schedule.NewTransferLeaderOperator(regionID, region.Leader, newLeader)
	c.addOperator(schedule.NewAdminOperator(region, op))
	return nil
}

// AddTransferRegionOperator adds an operator to transfer region to the stores.
func (h *Handler) AddTransferRegionOperator(regionID uint64, storeIDs map[uint64]struct{}) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}

	region := c.cluster.getRegion(regionID)
	if region == nil {
		return errRegionNotFound(regionID)
	}

	var ops []schedule.Operator

	// Add missing peers.
	for id := range storeIDs {
		if c.cluster.GetStore(id) == nil {
			return errStoreNotFound(id)
		}
		if region.GetStorePeer(id) != nil {
			continue
		}
		peer, err := c.cluster.AllocPeer(id)
		if err != nil {
			return errors.Trace(err)
		}
		ops = append(ops, schedule.NewAddPeerOperator(regionID, peer))
	}

	// Remove redundant peers.
	for _, peer := range region.GetPeers() {
		if _, ok := storeIDs[peer.GetStoreId()]; ok {
			continue
		}
		ops = append(ops, schedule.NewRemovePeerOperator(regionID, peer))
	}

	c.addOperator(schedule.NewAdminOperator(region, ops...))
	return nil
}

// AddTransferPeerOperator adds an operator to transfer peer.
func (h *Handler) AddTransferPeerOperator(regionID uint64, fromStoreID, toStoreID uint64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}

	region := c.cluster.getRegion(regionID)
	if region == nil {
		return errRegionNotFound(regionID)
	}

	oldPeer := region.GetStorePeer(fromStoreID)
	if oldPeer == nil {
		return errors.Errorf("region has no peer in store %v", fromStoreID)
	}

	if c.cluster.GetStore(toStoreID) == nil {
		return errStoreNotFound(toStoreID)
	}
	newPeer, err := c.cluster.AllocPeer(toStoreID)
	if err != nil {
		return errors.Trace(err)
	}

	addPeer := schedule.NewAddPeerOperator(regionID, newPeer)
	removePeer := schedule.NewRemovePeerOperator(regionID, oldPeer)
	c.addOperator(schedule.NewAdminOperator(region, addPeer, removePeer))
	return nil
}
