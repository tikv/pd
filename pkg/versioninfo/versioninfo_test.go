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

package versioninfo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsHotScheduleWithCPUSupported(t *testing.T) {
	re := require.New(t)
	re.False(IsHotScheduleWithCPUSupported(nil))

	tests := []struct {
		version string
		expect  bool
	}{
		{"8.5.5", false},
		{"8.5.6", true},
		{"9.0.0-beta.1", true},
		{"9.0.0", true},
		{"9.1.0", true},
	}
	for _, test := range tests {
		re.Equal(test.expect, IsHotScheduleWithCPUSupported(MustParseVersion(test.version)), test.version)
	}
}
