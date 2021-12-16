// Copyright 2021 TiKV Project Authors.
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

package base

import (
	"encoding/json"
	"path"

	"github.com/tikv/pd/pkg/errs"
)

const replicationPath = "replication_mode"

// ReplicationStatusStorage defines the storage operations on the replication status.
type ReplicationStatusStorage interface {
	LoadReplicationStatus(mode string, status interface{}) (bool, error)
	SaveReplicationStatus(mode string, status interface{}) error
}

// LoadReplicationStatus loads replication status by mode.
func (s *Storage) LoadReplicationStatus(mode string, status interface{}) (bool, error) {
	v, err := s.Load(path.Join(replicationPath, mode))
	if err != nil {
		return false, err
	}
	if v == "" {
		return false, nil
	}
	err = json.Unmarshal([]byte(v), status)
	if err != nil {
		return false, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByArgs()
	}
	return true, nil
}

// SaveReplicationStatus stores replication status by mode.
func (s *Storage) SaveReplicationStatus(mode string, status interface{}) error {
	value, err := json.Marshal(status)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByArgs()
	}
	return s.Save(path.Join(replicationPath, mode), string(value))
}
