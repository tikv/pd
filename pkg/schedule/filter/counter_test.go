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

package filter

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/schedule/types"
)

func TestString(t *testing.T) {
	re := require.New(t)
	testcases := []struct {
		filterType int
		expected   string
	}{
		{int(storeStateTombstone), "store-state-tombstone-filter"},
		{int(filtersLen - 1), "store-state-slow-trend-filter"},
		{int(filtersLen), "unknown"},
	}

	for _, data := range testcases {
		re.Equal(data.expected, filterType(data.filterType).String())
	}
	re.Len(filters, int(filtersLen))
}

func TestCounter(t *testing.T) {
	re := require.New(t)
	counter := NewCounter(types.BalanceLeaderScheduler.String())
	counter.inc(source, storeStateTombstone, 1, 2)
	counter.inc(target, storeStateTombstone, 1, 2)
	re.Equal(1, counter.counter[source][storeStateTombstone][1][2])
	re.Equal(1, counter.counter[target][storeStateTombstone][1][2])
	counter.Flush()
	re.Zero(counter.counter[source][storeStateTombstone][1][2])
	re.Zero(counter.counter[target][storeStateTombstone][1][2])
}
