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

package schedulers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/statistics/utils"
)

func TestAdjustPrioritiesConfigManualCPUOptIn(t *testing.T) {
	re := require.New(t)
	tests := []struct {
		name         string
		querySupport bool
		origins      []string
		expect       []string
	}{
		{
			name:         "query-supported-keeps-manual-cpu-priority",
			querySupport: true,
			origins:      []string{utils.CPUPriority, utils.BytePriority},
			expect:       []string{utils.CPUPriority, utils.BytePriority},
		},
		{
			name:         "query-supported-keeps-default-query-priority",
			querySupport: true,
			origins:      []string{utils.QueryPriority, utils.BytePriority},
			expect:       []string{utils.QueryPriority, utils.BytePriority},
		},
		{
			name:         "query-unsupported-falls-back-from-manual-cpu-priority",
			querySupport: false,
			origins:      []string{utils.CPUPriority, utils.BytePriority},
			expect:       getReadPriorities(&compatiblePrioritiesConfig),
		},
		{
			name:         "query-unsupported-with-query-origins",
			querySupport: false,
			origins:      []string{utils.QueryPriority, utils.BytePriority},
			expect:       getReadPriorities(&compatiblePrioritiesConfig),
		},
		{
			name:         "query-supported-malformed-priority-uses-defaults",
			querySupport: true,
			origins:      []string{"bad", "byte"},
			expect:       getReadPriorities(&defaultPrioritiesConfig),
		},
	}

	for _, test := range tests {
		got := adjustPrioritiesConfig(test.querySupport, test.origins, getReadPriorities)
		re.Equal(test.expect, got, test.name)
	}
}
