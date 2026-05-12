// Copyright 2019 TiKV Project Authors.
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

package tso_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

func TestTSO(t *testing.T) {
	re := require.New(t)
	cmd := ctl.GetRootCmd()

	const (
		physicalShiftBits = 18
		logicalBits       = 0x3FFFF
	)

	// tso command
	ts := "395181938313123110"
	args := []string{"tso", ts}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	tsTime, err := strconv.ParseUint(ts, 10, 64)
	re.NoError(err)
	logicalTime := tsTime & logicalBits
	physical := tsTime >> physicalShiftBits
	physicalTime := time.Unix(int64(physical/1000), int64(physical%1000)*time.Millisecond.Nanoseconds())
	str := fmt.Sprintln("system: ", physicalTime) + fmt.Sprintln("logic:  ", logicalTime)
	re.Equal(string(output), str)

	// test with invalid address
	args = []string{"-u", "127.0.0.1", "tso", ts}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	tsTime, err = strconv.ParseUint(ts, 10, 64)
	re.NoError(err)
	logicalTime = tsTime & logicalBits
	physical = tsTime >> physicalShiftBits
	physicalTime = time.Unix(int64(physical/1000), int64(physical%1000)*time.Millisecond.Nanoseconds())
	str = fmt.Sprintln("system: ", physicalTime) + fmt.Sprintln("logic:  ", logicalTime)
	re.Equal(string(output), str)
}
