// Copyright 2022 TiKV Project Authors.
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

package keyspace

import (
	"regexp"

	"github.com/pingcap/errors"
)

const (
	spaceIDMax = ^uint32(0) >> 8 // 16777215 (Uint24Max) is the maximum value of spaceID.
	// namePattern is a regex that specifies acceptable characters of the keyspace name.
	// Name must be non-empty and contains only alphanumerical, `_` and `-`.
	namePattern = "^[-A-Za-z0-9_]+$"
)

var (
	// ErrKeyspaceNotFound is used to indicate target keyspace does not exist.
	ErrKeyspaceNotFound = errors.New("keyspace does not exist")
	// ErrKeyspaceExists indicates target keyspace already exists.
	// Used when creating a new keyspace.
	ErrKeyspaceExists   = errors.New("keyspace already exists")
	errKeyspaceArchived = errors.New("keyspace already archived")
	errArchiveEnabled   = errors.New("cannot archive ENABLED keyspace")
	errModifyDefault    = errors.New("cannot modify default keyspace's state")
	errIllegalOperation = errors.New("unknown operation")
)

// validateID check if keyspace falls within the acceptable range.
// It throws errIllegalID when input id is our of range,
// or if it collides with reserved id.
func validateID(spaceID uint32) error {
	if spaceID > spaceIDMax {
		return errors.Errorf("illegal keyspace id %d, larger than spaceID Max %d", spaceID, spaceIDMax)
	}
	if spaceID == DefaultKeyspaceID {
		return errors.Errorf("illegal keyspace id %d, collides with default keyspace id", spaceID)
	}
	return nil
}

// validateName check if user provided name is legal.
// It throws errIllegalName when name contains illegal character,
// or if it collides with reserved name.
func validateName(name string) error {
	isValid, err := regexp.MatchString(namePattern, name)
	if err != nil {
		return err
	}
	if !isValid {
		return errors.Errorf("illegal keyspace name %s, should contain only alphanumerical and underline", name)
	}
	if name == DefaultKeyspaceName {
		return errors.Errorf("illegal keyspace name %s, collides with default keyspace name", name)
	}
	return nil
}

// SpaceIDHash is used to hash the spaceID inside the lockGroup.
// A simple mask is applied to spaceID to use its last byte as map key,
// limiting the maximum map length to 256.
// Since keyspaceID is sequentially allocated, this can also reduce the chance
// of collision when comparing with random hashes.
func SpaceIDHash(spaceID uint32) uint32 {
	return spaceID & 0xFF
}
