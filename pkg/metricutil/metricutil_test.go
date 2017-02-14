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

package metricutil

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/typeutil"
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

func (s *testMetricsSuite) TestGetCmdLabel(c *C) {
	requests := make([]*pdpb.Request, 0, 2)
	labels := make([]string, 0, 2)

	r := new(pdpb.Request)
	r.CmdType = pdpb.CommandType_Tso
	requests = append(requests, r)
	labels = append(labels, "tso")

	// Invalid CommandType
	r = new(pdpb.Request)
	r.CmdType = pdpb.CommandType(-1)
	requests = append(requests, r)
	labels = append(labels, "-1")

	for i, r := range requests {
		l := GetCmdLabel(r)
		c.Assert(l, Equals, labels[i])
	}
}

// Seems useless, but improves coverage.
func (s *testMetricsSuite) TestCoverage(c *C) {
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
