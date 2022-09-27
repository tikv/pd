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
)

func TestString(t *testing.T) {
	re := require.New(t)
	testcases := []struct {
		filterType int
		expected   string
	}{
		{int(StoreStateFilterType), "store-state--filter"},
		{int(StoreStateFilterType + 1), "store-state-tombstone-filter"},
		{int(FiltersLen - 1), "store-state-reject-leader-filter"},
	}

	for _, data := range testcases {
		re.Equal(data.expected, filterType(data.filterType).String())
	}
}
