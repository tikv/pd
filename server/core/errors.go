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
	"fmt"
	"net/http"

	"github.com/pingcap/pd/server/error_code"
)

const (
	// StoreTombstonedCode is an invalid operation was attempted on a store which is in a removed state.
	StoreTombstonedCode errcode.RegisteredCode = "store.state.tombstoned"
	// StoreBlockedCode is an error due to requesting an operation that is invalid due to a store being in a blocked state
	StoreBlockedCode errcode.RegisteredCode = "store.state.blocked"
)

// StoreTombstoned is an invalid operation was attempted on a store which is in a removed state.
type StoreTombstoned struct {
	StoreID   uint64 `json:"storeId"`
	Operation string `json:"operation"`
}

func (e StoreTombstoned) Error() string {
	return fmt.Sprintf("The store %020d has been removed and the operation %s is invalid", e.StoreID, e.Operation)
}

var _ errcode.ErrorCode = (*StoreTombstoned)(nil)   // assert implements interface
var _ errcode.HasHTTPCode = (*StoreTombstoned)(nil) // assert implements interface

// GetHTTPCode returns 410
func (e StoreTombstoned) GetHTTPCode() int {
	return http.StatusGone
}

// Code returns StoreTombstonedCode
func (e StoreTombstoned) Code() errcode.RegisteredCode {
	return StoreTombstonedCode
}

// StoreBlocked has a Code() of StoreBlockedCode
type StoreBlocked struct {
	StoreID uint64
}

func (e StoreBlocked) Error() string {
	return fmt.Sprintf("store %v is blocked", e.StoreID)
}

var _ errcode.ErrorCode = (*StoreBlocked)(nil) // assert implements interface

// Code returns StoreBlockedCode
func (e StoreBlocked) Code() errcode.RegisteredCode {
	return StoreBlockedCode
}
