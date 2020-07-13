// Copyright 2018 PingCAP, Inc.
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

// Package core defines core characteristics of the server.
// This file uses the errcode packate to define PD specific error codes.
// Probably this should be a different package.
package core

import (
	"github.com/joomcode/errorx"
)

var (
	coreErrors = errorx.NewNamespace("core")
	// ErrStoreNotFound is error info for store not found.
	ErrStoreNotFound = coreErrors.NewType("store_not_found")
	// ErrStoreTombstoned is error info for store tombstoned.
	ErrStoreTombstoned = coreErrors.NewType("store_tombstone")
	// ErrStorePauseLeaderTransfer is error info for store pausing leader transfer.
	ErrStorePauseLeaderTransfer = coreErrors.NewType("store_pause_leader_transfer")
	// ErrRegionIsStale is error info for region stale.
	ErrRegionIsStale = coreErrors.NewType("region_is_stale")
	// ErrInvalidData is error info for invalid data.
	ErrInvalidData = coreErrors.NewType("invalid_data")

	// NewStoreNotFoundErr is a wrap function for ErrStoreNotFound
	NewStoreNotFoundErr = func(storeID uint64) error {
		return ErrStoreNotFound.New("store id: %d", storeID)
	}
	// NewStoreTombstonedErr is a wrap function for ErrStoreTombstoned
	NewStoreTombstonedErr = func(storeID uint64) error {
		return ErrStoreTombstoned.New("store id: %d", storeID)
	}
)
