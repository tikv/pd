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
	"testing"

	"github.com/stretchr/testify/require"
)

type testJSONStructLevel1 struct {
	Name string               `json:"name"`
	Sub1 testJSONStructLevel2 `json:"sub1"`
	Sub2 testJSONStructLevel2 `json:"sub2"`
}

type testJSONStructLevel2 struct {
	SubName string `json:"sub-name"`
}

func TestJSONUtil(t *testing.T) {
	re := require.New(t)
	father := &testJSONStructLevel1{
		Name: "father",
	}
	son1 := &testJSONStructLevel2{
		SubName: "son1",
	}
	update, found, err := AddKeyValue(&father, "sub1", &son1)
	re.NoError(err)
	re.True(update)
	re.True(found)

	son2 := &testJSONStructLevel2{
		SubName: "son2",
	}

	update, found, err = AddKeyValue(father, "sub2", &son2)
	re.NoError(err)
	re.True(update)
	re.True(found)

	update, found, err = AddKeyValue(father, "sub3", &son2)
	re.NoError(err)
	re.False(update)
	re.False(found)

	update, found, err = AddKeyValue(father, "sub2", &son2)
	re.NoError(err)
	re.False(update)
	re.True(found)
}
