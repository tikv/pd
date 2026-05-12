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

package endpoint

import (
	"encoding/json"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// ReplicationStatusStorage defines the storage operations on the replication status.
type ReplicationStatusStorage interface {
	LoadReplicationStatus(mode string, status any) (bool, error)
	SaveReplicationStatus(mode string, status any) error
}

var _ ReplicationStatusStorage = (*StorageEndpoint)(nil)

// LoadReplicationStatus loads replication status by mode.
func (se *StorageEndpoint) LoadReplicationStatus(mode string, status any) (bool, error) {
	v, err := se.Load(keypath.ReplicationModePath(mode))
	if err != nil || v == "" {
		return false, err
	}
	err = json.Unmarshal([]byte(v), status)
	if err != nil {
		return false, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByArgs()
	}
	return true, nil
}

// SaveReplicationStatus stores replication status by mode.
func (se *StorageEndpoint) SaveReplicationStatus(mode string, status any) error {
	return se.saveJSON(keypath.ReplicationModePath(mode), status)
}
