// Copyright 2018 TiKV Project Authors.
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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateURLWithScheme(t *testing.T) {
	re := require.New(t)
	tests := []struct {
		addr   string
		hasErr bool
	}{
		{"", true},
		{"foo", true},
		{"/foo", true},
		{"http", true},
		{"http://", true},
		{"http://foo", false},
		{"https://foo", false},
		{"http://127.0.0.1", false},
		{"http://127.0.0.1/", false},
		{"http://[::1]:2379", false},
		// Ambiguous host syntaxes whose parse result depends on GODEBUG
		// urlstrictcolons, which defaults to strict only when the main module
		// declares go >= 1.26, so they are not asserted here:
		//   - "http://::1:2379"
		//   - "http://localhost:80:80"
		{"https://foo.com/bar", false},
		{"https://foo.com/bar/", false},
	}
	for _, test := range tests {
		re.Equal(test.hasErr, ValidateURLWithScheme(test.addr) != nil)
	}
}
