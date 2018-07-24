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

package server

import (
	"math/rand"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (s *testUtilSuite) TestParseTimestap(c *C) {
	for i := 0; i < 3; i++ {
		t := time.Now().Add(time.Second * time.Duration(rand.Int31n(1000)))
		data := uint64ToBytes(uint64(t.UnixNano()))
		nt, err := parseTimestamp(data)
		c.Assert(err, IsNil)
		c.Assert(nt.Equal(t), IsTrue)
	}
	data := []byte("pd")
	nt, err := parseTimestamp(data)
	c.Assert(err, NotNil)
	c.Assert(nt.Equal(zeroTime), IsTrue)
}

func (s *testUtilSuite) TestSubTimeByWallClock(c *C) {
	for i := 0; i < 3; i++ {
		r := rand.Int31n(1000)
		t1 := time.Now()
		t2 := t1.Add(time.Second * time.Duration(r))
		duration := subTimeByWallClock(t2, t1)
		c.Assert(duration, Equals, time.Second*time.Duration(r))
	}
}

func (s *testUtilSuite) TestVerifyLabels(c *C) {
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
		{"192.168.199.1", false},
		{"www.pingcap.com", false},
		{"h_127.0.0.1", false},
		{"a", false},
	}
	for _, t := range tests {
		c.Assert(ValidateLabelString(t.label) != nil, Equals, t.hasErr)
	}
}
