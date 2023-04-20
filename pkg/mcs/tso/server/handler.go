// Copyright 2023 TiKV Project Authors.
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

package server

import (
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/tso"
	"go.uber.org/zap"
)

// Handler is a helper to export methods to handle API/RPC requests.
type Handler struct {
	s *Server
}

func newHandler(s *Server) *Handler {
	return &Handler{s: s}
}

// ResetTS resets the TSO with the specified one.
func (h *Handler) ResetTS(
	ts uint64, ignoreSmaller, skipUpperBoundCheck bool, keyspaceGroupID uint32,
) error {
	log.Info("reset-ts",
		zap.Uint64("new-ts", ts),
		zap.Bool("ignore-smaller", ignoreSmaller),
		zap.Bool("skip-upper-bound-check", skipUpperBoundCheck),
		zap.Uint32("keyspace-group-id", keyspaceGroupID))
	tsoAllocatorManager, err := h.s.GetTSOAllocatorManager(keyspaceGroupID)
	if err != nil {
		log.Error("failed to get allocator manager", errs.ZapError(err))
		return err
	}
	tsoAllocator, err := tsoAllocatorManager.GetAllocator(tso.GlobalDCLocation)
	if err != nil {
		return err
	}
	if tsoAllocator == nil {
		return errs.ErrServerNotStarted
	}
	return tsoAllocator.SetTSO(ts, ignoreSmaller, skipUpperBoundCheck)
}
