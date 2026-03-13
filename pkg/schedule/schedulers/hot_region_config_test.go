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

func TestAdjustPrioritiesConfigCPUFallback(t *testing.T) {
	re := require.New(t)
	tests := []struct {
		name         string
		querySupport bool
		cpuSupport   bool
		origins      []string
		expect       []string
	}{
		{
			name:         "cpu-supported-keep-origins",
			querySupport: true,
			cpuSupport:   true,
			origins:      []string{utils.CPUPriority, utils.BytePriority},
			expect:       getReadPriorities(&defaultPrioritiesConfig),
		},
		{
			name:         "cpu-unsupported-fallback-to-query",
			querySupport: true,
			cpuSupport:   false,
			origins:      []string{utils.CPUPriority, utils.BytePriority},
			expect:       getReadPriorities(&queryPrioritiesConfig),
		},
		{
			name:         "query-unsupported-fallback-to-compatible",
			querySupport: false,
			cpuSupport:   true,
			origins:      []string{utils.CPUPriority, utils.BytePriority},
			expect:       getReadPriorities(&compatiblePrioritiesConfig),
		},
		{
			name:         "query-unsupported-with-query-origins",
			querySupport: false,
			cpuSupport:   false,
			origins:      []string{utils.QueryPriority, utils.BytePriority},
			expect:       getReadPriorities(&compatiblePrioritiesConfig),
		},
	}

	for _, test := range tests {
		got := adjustPrioritiesConfig(test.querySupport, test.cpuSupport, test.origins, getReadPriorities)
		re.Equal(test.expect, got, test.name)
	}
}
