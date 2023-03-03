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

package configutil

import (
	"bytes"
	"testing"
)

func TestPrintConfigCheckMsg(t *testing.T) {
	// Define test cases
	tests := []struct {
		name        string
		warningMsgs []string
		want        string
	}{
		{
			name:        "no warnings",
			warningMsgs: []string{},
			want:        "config check successful\n",
		},
		{
			name:        "with warnings",
			warningMsgs: []string{"warning message 1", "warning message 2"},
			want:        "warning message 1\nwarning message 2\n",
		},
	}

	// Run tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Redirect output to a buffer to capture the output
			var buf bytes.Buffer
			PrintConfigCheckMsg(&buf, tt.warningMsgs)

			// Compare the output to the expected value
			if got := buf.String(); got != tt.want {
				t.Errorf("PrintConfigCheckMsg() = %q, want %q", got, tt.want)
			}
		})
	}
}
