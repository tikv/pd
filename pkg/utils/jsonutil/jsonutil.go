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

package jsonutil

import (
	"bytes"
	"encoding/json"

	"github.com/tikv/pd/pkg/utils/reflectutil"
)

// AddKeyValue is used to add a key value pair into `old`
func AddKeyValue(old any, key string, value any) (updated bool, found bool, err error) {
	data, err := json.Marshal(map[string]any{key: value})
	if err != nil {
		return false, false, err
	}
	return MergeJSONObject(old, data)
}

// MergeJSONObject is used to merge a marshaled json object into v
func MergeJSONObject(v any, data []byte) (updated bool, found bool, err error) {
	old, _ := json.Marshal(v)
	if err := json.Unmarshal(data, v); err != nil {
		return false, false, err
	}
	new, _ := json.Marshal(v)
	if !bytes.Equal(old, new) {
		return true, true, nil
	}
	m := make(map[string]any)
	if err := json.Unmarshal(data, &m); err != nil {
		return false, false, err
	}
	found = reflectutil.FindSameFieldByJSON(v, m)
	return false, found, nil
}
