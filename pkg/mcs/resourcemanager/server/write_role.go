// Copyright 2026 TiKV Project Authors.
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
	stderrors "errors"

	"github.com/pingcap/errors"
)

// ResourceGroupWriteRole controls which type of data is allowed to be persisted.
type ResourceGroupWriteRole uint8

const (
	// ResourceGroupWriteRoleLegacyAll keeps the legacy behavior and allows both metadata and state writes.
	ResourceGroupWriteRoleLegacyAll ResourceGroupWriteRole = iota
	// ResourceGroupWriteRolePDMetaOnly allows metadata writes only.
	ResourceGroupWriteRolePDMetaOnly
	// ResourceGroupWriteRoleRMTokenOnly allows state writes only.
	ResourceGroupWriteRoleRMTokenOnly
)

// AllowsMetadataWrite returns whether metadata writes are allowed in this role.
func (r ResourceGroupWriteRole) AllowsMetadataWrite() bool {
	return r == ResourceGroupWriteRoleLegacyAll || r == ResourceGroupWriteRolePDMetaOnly
}

// AllowsStateWrite returns whether running state writes are allowed in this role.
func (r ResourceGroupWriteRole) AllowsStateWrite() bool {
	return r == ResourceGroupWriteRoleLegacyAll || r == ResourceGroupWriteRoleRMTokenOnly
}

var errMetadataWriteDisabled = errors.New("metadata write is disabled")

// IsMetadataWriteDisabledError reports whether err is a metadata-write-disabled error.
func IsMetadataWriteDisabledError(err error) bool {
	return stderrors.Is(err, errMetadataWriteDisabled)
}
