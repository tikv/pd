// Copyright 2023 TiKV Project Authors.
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

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegionTreeStoreInterface(t *testing.T) {
	re := require.New(t)
	tree := NewRegionTree()

	regionA := NewTestItem([]byte("a"), []byte("b"))
	tree.Add(regionA)
	res, _ := tree.List("a", "z", 10)
	re.Len(res, 1)
	regionB := NewTestItem([]byte("b"), []byte("c"))
	tree.Add(regionB)
	res, _ = tree.List("a", "z", 10)
	re.Len(res, 2)
	regionC := NewTestItem([]byte("a"), []byte("c"))
	tree.Update(regionC)
	res, _ = tree.List("a", "z", 10)
	re.Len(res, 1)
	r, _ := tree.GetByKey("a")
	expected := NewRegionInfo(regionC.GetRegionStat().GetRegion(), regionC.GetRegionStat().GetLeader())
	re.Equal(expected, r)
	tree.Delete(regionC)
	res, _ = tree.List("a", "z", 10)
	re.Len(res, 0)
	r, _ = tree.GetByKey("a")
	re.Empty(r)
}
