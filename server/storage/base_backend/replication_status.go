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

package backend

import (
	"encoding/json"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/storage/endpoint"
)

var _ endpoint.ReplicationStatusStorage = (*BaseBackend)(nil)

// LoadReplicationStatus loads replication status by mode.
func (bb *BaseBackend) LoadReplicationStatus(mode string, status interface{}) (bool, error) {
	v, err := bb.Load(replicationModePath(mode))
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
func (bb *BaseBackend) SaveReplicationStatus(mode string, status interface{}) error {
	value, err := json.Marshal(status)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByArgs()
	}
	return bb.Save(replicationModePath(mode), string(value))
}
