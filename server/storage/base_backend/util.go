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
	"path"

	"github.com/gogo/protobuf/proto"
	"github.com/tikv/pd/pkg/errs"
)

func (bb *BaseBackend) loadProto(key string, msg proto.Message) (bool, error) {
	value, err := bb.Load(key)
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
	}
	err = proto.Unmarshal([]byte(value), msg)
	if err != nil {
		return false, errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return true, nil
}

func (bb *BaseBackend) saveProto(key string, msg proto.Message) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
	}
	return bb.Save(key, string(value))
}

func (bb *BaseBackend) saveJSON(prefix, key string, data interface{}) error {
	value, err := json.Marshal(data)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByArgs()
	}
	return bb.Save(path.Join(prefix, key), string(value))
}
