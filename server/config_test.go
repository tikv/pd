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

package server

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (s *testConfigSuite) TestParseConstraint(c *C) {
	cfg := &ConstraintConfig{}
	_, err := parseConstraint(cfg)
	c.Assert(err, NotNil)

	cfg.Replicas = 1
	constraint, err := parseConstraint(cfg)
	c.Assert(err, IsNil)
	c.Assert(constraint.Labels, HasLen, 0)
	c.Assert(constraint.Replicas, Equals, 1)

	invalidLabels := [][]string{
		{"abc"},
		{"abc="},
		{"abc.012"},
		{"abc,012"},
		{"abc=123*"},
		{".123=-abc"},
		{"abc,123=123.abc"},
		{"abc=123", "abc=abc"},
	}
	for _, labels := range invalidLabels {
		cfg.Labels = labels
		_, err = parseConstraint(cfg)
		c.Assert(err, NotNil)
	}

	cfg.Labels = []string{"a=0"}
	constraint, err = parseConstraint(cfg)
	c.Assert(err, IsNil)
	c.Assert(constraint.Labels["a"], Equals, "0")
	c.Assert(constraint.Replicas, Equals, 1)

	cfg.Labels = []string{"a.1-2=b_1.2", "cab-012=3ac.8b2"}
	constraint, err = parseConstraint(cfg)
	c.Assert(err, IsNil)
	c.Assert(constraint.Labels["a.1-2"], Equals, "b_1.2")
	c.Assert(constraint.Labels["cab-012"], Equals, "3ac.8b2")

	cfg.Labels = []string{"zone=us-west-1", "disk=ssd", "Test=Test"}
	constraint, err = parseConstraint(cfg)
	c.Assert(err, IsNil)
	c.Assert(constraint.Labels["zone"], Equals, "us-west-1")
	c.Assert(constraint.Labels["disk"], Equals, "ssd")
	c.Assert(constraint.Labels["test"], Equals, "test")
}
