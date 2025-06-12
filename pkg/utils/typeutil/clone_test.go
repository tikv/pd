// Copyright 2024 TiKV Project Authors.
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

package typeutil

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

type fate struct {
	ID   uint64
	Attr struct {
		Age int64
	}
}

func (f *fate) Marshal() ([]byte, error) {
	return json.Marshal(f)
}

func (f *fate) Unmarshal(data []byte) error {
	return json.Unmarshal(data, f)
}

var fateFactory = func() *fate { return &fate{} }

func TestDeepClone(t *testing.T) {
	re := assert.New(t)
	src := &fate{ID: 1}
	dst := DeepClone(src, fateFactory)
	re.EqualValues(1, dst.ID)
	dst.ID = 2
	re.EqualValues(1, src.ID)

	// case2: the source is nil
	var src2 *fate
	re.Nil(DeepClone(src2, fateFactory))
}
