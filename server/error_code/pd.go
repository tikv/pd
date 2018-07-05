// Package errcode defines error codes
// This file defines PD specific error codes.
// Probably this should be a different package.
package errcode

import (
	"fmt"
	"net/http"
)

// StoreTombstoned is an invalid operation was attempted on a store which is in a removed state.
type StoreTombstoned struct {
	StoreID   uint64 `json:"storeId"`
	Operation string `json:"operation"`
}

const (
	// StoreTombstonedCode is an invalid operation was attempted on a store which is in a removed state.
	StoreTombstonedCode RegisteredCode = "store.state.tombstoned"
)

var _ ErrorCode = (*StoreTombstoned)(nil) // assert implements interface

func (e StoreTombstoned) Error() string {
	return fmt.Sprintf("The store %020d has been removed and the operation %s is invalid", e.StoreID, e.Operation)
}

// HTTPCode returns 410
func (e StoreTombstoned) HTTPCode() int {
	return http.StatusGone
}

// Code returns StoreTombstonedCode
func (e StoreTombstoned) Code() RegisteredCode {
	return StoreTombstonedCode
}
