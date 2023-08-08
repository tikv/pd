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

package config

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
)

func TestValidateLabels(t *testing.T) {
	re := require.New(t)
	tests := []struct {
		label  string
		hasErr bool
	}{
		{"z1", false},
		{"z-1", false},
		{"h1;", true},
		{"z_1", false},
		{"z_1&", true},
		{"cn", false},
		{"Zo^ne", true},
		{"z_", true},
		{"hos&t-15", true},
		{"_test1", true},
		{"-test1", true},
		{"127.0.0.1", false},
		{"www.pingcap.com", false},
		{"h_127.0.0.1", false},
		{"a", false},
		{"a/b", false},
		{"ab/", true},
		{"/ab", true},
		{"$abc", false},
		{"$", true},
		{"a$b", true},
		{"$$", true},
	}
	for _, test := range tests {
		re.Equal(test.hasErr, ValidateLabels([]*metapb.StoreLabel{{Key: test.label}}) != nil)
	}
}
