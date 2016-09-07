// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMetricsSuite{})

type testMetricsSuite struct {
}

func (s *testMetricsSuite) TestConvertName(c *C) {
	inputs := []struct {
		name    string
		newName string
	}{
		{"Abc", "abc"},
		{"aAbc", "a_abc"},
		{"ABc", "a_bc"},
		{"AbcDef", "abc_def"},
		{"AbcdefghijklmnopqrstuvwxyzAbcdefghijklmnopqrstuvwxyzAbcdefghijklmnopqrstuvwxyz",
			"abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz"},
	}

	for _, input := range inputs {
		c.Assert(input.newName, Equals, convertName(input.name))
	}
}
