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

package simulator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegionChangeSnapshot(t *testing.T) {
	re := require.New(t)
	engine := &RaftEngine{regionChange: map[uint64][]uint64{1: {11, 12}}}

	var reported []uint64
	for _, regionID := range engine.GetRegionChange(1) {
		reported = append(reported, regionID)
		engine.ResetRegionChange(1, regionID)
	}

	re.Equal([]uint64{11, 12}, reported)
	re.Empty(engine.GetRegionChange(1))
	re.Empty(engine.GetRegionChange(2))
}
