// Copyright 2018 PingCAP, Inc.
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

package placement

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestPlacement(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testPlacementSuite{})

type testPlacementSuite struct{}

func (s *testPlacementSuite) TestConfigParse(c *C) {
	cases := []struct {
		source string
		config *Config
	}{
		{source: ``, config: &Config{}},
		{source: `;`, config: &Config{}},
		{
			source: `count()=3`,
			config: s.config(s.constraint("count", "=", 3)),
		},
		{
			source: `count(zone:z1,region:r3)>=5`,
			config: s.config(s.constraint("count", ">=", 5, "zone", "z1", "region", "r3")),
		},
		{
			source: `count()>5;label_values(zone:z1,host,ssd)>-1`,
			config: s.config(s.constraint("count", ">", 5), s.constraint("label_values", ">", -1, "zone", "z1", "host", "", "ssd", "")),
		},
		{
			source: " count ( )  <5 ;; \tlabel_values\t ( zone : z1 , \thost , ssd ) > -1 ;;;",
			config: s.config(s.constraint("count", "<", 5), s.constraint("label_values", ">", -1, "zone", "z1", "host", "", "ssd", "")),
		},
		// Wrong format configs.
		{source: "count=3"},
		{source: "count()=abc"},
		{source: "count()=<3"},
		{source: "count()"},
		{source: "+count()"},
		{source: "count(a=b)=1"},
		{source: "count(a;b;c)=1"},
	}

	for _, t := range cases {
		config, err := ParseConfig(t.source)
		if t.config != nil {
			c.Assert(err, IsNil)
			c.Assert(config, DeepEquals, t.config)
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testPlacementSuite) constraint(function string, op string, value int, argPairs ...string) *Constraint {
	var filters []Filter
	var labels []string
	for i := 0; i < len(argPairs); i += 2 {
		if argPairs[i+1] == "" {
			labels = append(labels, argPairs[i])
		} else {
			filters = append(filters, Filter{Key: argPairs[i], Value: argPairs[i+1]})
		}
	}
	return &Constraint{
		Function: function,
		Filters:  filters,
		Labels:   labels,
		Op:       op,
		Value:    value,
	}
}

func (s *testPlacementSuite) config(constraints ...*Constraint) *Config {
	return &Config{Constraints: constraints}
}
