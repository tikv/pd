// Copyright 2016 TiKV Project Authors.
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

package metricutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/typeutil"
)

func TestCamelCaseToSnakeCase(t *testing.T) {
	re := require.New(t)
	inputs := []struct {
		name    string
		newName string
	}{
		{"a", "a"},
		{"snake", "snake"},
		{"A", "a"},
		{"ID", "id"},
		{"MOTD", "motd"},
		{"Snake", "snake"},
		{"SnakeTest", "snake_test"},
		{"SnakeID", "snake_id"},
		{"SnakeIDGoogle", "snake_id_google"},
		{"GetPDMembers", "get_pd_members"},
		{"LinuxMOTD", "linux_motd"},
		{"OMGWTFBBQ", "omgwtfbbq"},
		{"omg_wtf_bbq", "omg_wtf_bbq"},
		{"Abc", "abc"},
		{"aAbc", "a_abc"},
		{"aBCd", "a_b_cd"},
		{"ABc", "a_bc"},
		{"AbcDef", "abc_def"},
		{"AbcdefghijklmnopqrstuvwxyzAbcdefghijklmnopqrstuvwxyzAbcdefghijklmnopqrstuvwxyz",
			"abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz"},
	}

	for _, input := range inputs {
		re.Equal(input.newName, camelCaseToSnakeCase(input.name))
	}
}

func TestCoverage(_ *testing.T) {
	cfgs := []*MetricConfig{
		{
			PushJob:     "j1",
			PushAddress: "127.0.0.1:9091",
			PushInterval: typeutil.Duration{
				Duration: time.Hour,
			},
		},
		{
			PushJob:     "j2",
			PushAddress: "127.0.0.1:9091",
			PushInterval: typeutil.Duration{
				Duration: zeroDuration,
			},
		},
	}

	for _, cfg := range cfgs {
		Push(cfg)
	}
}
