// Copyright 2019 PingCAP, Inc.
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
package nodeutil

import (
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetHost(t *testing.T) {
	type testCase struct {
		addr string
		host string
		port int
	}
	tcs := []*testCase{
		{"http://127.0.0.1:2379/", "127.0.0.1", 2379},
		{"http://127.0.0.1:2379", "127.0.0.1", 2379},
		{"https://127.0.0.1:2379/", "127.0.0.1", 2379},
		{"https://127.0.0.1:2379", "127.0.0.1", 2379},
	}

	for _, tc := range tcs {
		h, p, err := getIPPort(tc.addr)
		require.Nil(t, err)
		require.EqualValues(t, tc.host, h)
		require.EqualValues(t, tc.port, p)
	}
}

func TestParseCPU(t *testing.T) {
	reTwoColumns := regexp.MustCompile("\t+: ")
	sls := "cpu MHz		: 2499.994"
	sl := reTwoColumns.Split(sls, 2)
	t.Log(sl[0] == "cpu MHz")
	if speed, err := strconv.ParseFloat(sl[1], 64); err == nil {
		t.Log(speed)
	}
}
