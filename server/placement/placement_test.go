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
	"github.com/pingcap/pd/pkg/mock/mockcluster"
	"github.com/pingcap/pd/pkg/mock/mockoption"
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

func (s *testPlacementSuite) TestFunctions(c *C) {
	opt := mockoption.NewScheduleOptions()
	cluster := mockcluster.NewCluster(opt)
	cluster.PutStoreWithLabels(1, "zone", "z1", "host", "h1", "disk", "ssd")
	cluster.PutStoreWithLabels(2, "zone", "z1", "host", "h1", "disk", "ssd")
	cluster.PutStoreWithLabels(3, "zone", "z1", "host", "h2", "disk", "hdd")
	cluster.PutStoreWithLabels(4, "zone", "z2", "host", "h1", "disk", "ssd")

	cases := []struct {
		config       string
		regionStores []uint64
	}{
		{"count()=3", []uint64{1, 2, 3}},
		{"count()=1", []uint64{3}},
		{"count(zone:z1)=3", []uint64{1, 2, 3, 4}},
		{"count(zone:z2,host:h2)=0", []uint64{1, 2, 3, 4}},
		{"count(disk:ssd,zone,host)=3", []uint64{1, 2, 3, 4}},
		{"count(foo:bar)=0", []uint64{1, 2, 3, 4}},
		{"label_values()=0", []uint64{1, 2, 3, 4}},
		{"label_values(disk)=2", []uint64{1, 2, 3, 4}},
		{"label_values(disk)=1", []uint64{1, 2}},
		{"label_values(foo)=1", []uint64{1, 2, 3, 4}}, // All replicas have empty value for key 'foo'.
		{"label_values(zone)=2", []uint64{1, 2, 3, 4}},
		{"label_values(zone,host)=3", []uint64{1, 2, 3, 4}},
		{"label_values(zone:z2,host)=1", []uint64{1, 2, 3, 4}},
		{"label_values(zone:z1,host)=2", []uint64{1, 2, 3, 4}},
		{"label_values(zone:z1,host,disk)=2", []uint64{1, 2, 3, 4}},
		{"count_leader()=1", []uint64{1, 2, 3, 4}},
		{"count_leader(zone:z2)=0", []uint64{1, 2, 3, 4}},
		{"count_leader(zone:z1,disk:ssd)=1", []uint64{1, 2, 3, 4}},
		{"count_leader(zone:z1,disk:ssd)=0", []uint64{3, 1, 2, 4}},
		{"isolation_level(zone)=0", []uint64{1, 2}},
		{"isolation_level(zone)=1", []uint64{1, 4}},
		{"isolation_level(zone,host)=2", []uint64{1, 4}},
		{"isolation_level()=0", []uint64{1, 4}},
		{"isolation_level(zone,host)=1", []uint64{2, 3, 4}},
		{"isolation_level(zone,host,disk)=2", []uint64{2, 3, 4}},
		{"isolation_level(zone,host,disk)=0", []uint64{1, 2}},
		{"isolation_level(host)=0", []uint64{2, 4}},
		{"isolation_level(host,zone)=1", []uint64{2, 4}},
	}

	for _, t := range cases {
		constraint, err := parseConstraint(t.config)
		c.Assert(err, IsNil)
		cluster.PutRegionStores(1, t.regionStores...)
		c.Assert(constraint.Score(cluster.GetRegion(1), cluster), Equals, 0)
	}
}

func (s *testPlacementSuite) TestScore(c *C) {
	opt := mockoption.NewScheduleOptions()
	cluster := mockcluster.NewCluster(opt)
	cluster.PutStoreWithLabels(1)
	cluster.PutStoreWithLabels(2)
	cluster.PutStoreWithLabels(3)
	cluster.PutRegionStores(1, 1, 2, 3)

	cases := []struct {
		config string
		score  int
	}{
		{"count()=1", -2},
		{"count()=2", -1},
		{"count()=3", 0},
		{"count()=4", -1},
		{"count()=5", -2},

		{"count()<=1", -2},
		{"count()<=2", -1},
		{"count()<=3", 0},
		{"count()<=4", 1},
		{"count()<=5", 2},

		{"count()<1", -3},
		{"count()<2", -2},
		{"count()<3", -1},
		{"count()<4", 0},
		{"count()<5", 1},

		{"count()>=1", 2},
		{"count()>=2", 1},
		{"count()>=3", 0},
		{"count()>=4", -1},
		{"count()>=5", -2},

		{"count()>1", 1},
		{"count()>2", 0},
		{"count()>3", -1},
		{"count()>4", -2},
		{"count()>5", -3},
	}

	for _, t := range cases {
		constraint, err := parseConstraint(t.config)
		c.Assert(err, IsNil)
		c.Assert(constraint.Score(cluster.GetRegion(1), cluster), Equals, t.score)
	}
}
