// Copyright 2026 TiKV Project Authors.
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

package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParsePlanExecutionTimeout(t *testing.T) {
	re := require.New(t)

	timeout, err := parsePlanExecutionTimeout(map[string]any{})
	re.NoError(err)
	re.Zero(timeout)

	timeout, err = parsePlanExecutionTimeout(map[string]any{"plan-execution-timeout": float64(300)})
	re.NoError(err)
	re.Equal(5*time.Minute, timeout)

	timeout, err = parsePlanExecutionTimeout(map[string]any{
		"plan-execution-timeout": maxPlanExecutionTimeoutSeconds,
	})
	re.NoError(err)
	re.Equal(time.Duration(int64(maxPlanExecutionTimeoutSeconds))*time.Second, timeout)

	for _, input := range []map[string]any{
		{"plan-execution-timeout": float64(0)},
		{"plan-execution-timeout": float64(-1)},
		{"plan-execution-timeout": 1.5},
		{"plan-execution-timeout": maxPlanExecutionTimeoutSeconds + 1},
		{"plan-execution-timeout": "60"},
	} {
		_, err = parsePlanExecutionTimeout(input)
		re.Error(err)
	}

	_, err = parsePlanExecutionTimeout(map[string]any{
		"plan-execution-timeout": float64(300),
		"plan_execution_timeout": float64(600),
	})
	re.EqualError(err, "plan-execution-timeout is specified multiple times")
}

func TestParseTimeout(t *testing.T) {
	re := require.New(t)

	timeout, err := parseTimeout(map[string]any{})
	re.NoError(err)
	re.Equal(uint64(600), timeout)

	timeout, err = parseTimeout(map[string]any{"timeout": float64(300)})
	re.NoError(err)
	re.Equal(uint64(300), timeout)

	timeout, err = parseTimeout(map[string]any{"timeout": maxPlanExecutionTimeoutSeconds})
	re.NoError(err)
	re.Equal(uint64(maxPlanExecutionTimeoutSeconds), timeout)

	for _, input := range []map[string]any{
		{"timeout": float64(0)},
		{"timeout": float64(-1)},
		{"timeout": 1.5},
		{"timeout": maxPlanExecutionTimeoutSeconds + 1},
		{"timeout": "60"},
	} {
		_, err = parseTimeout(input)
		re.EqualError(err, "timeout is invalid")
	}
}

func TestParseDisableParanoidCheck(t *testing.T) {
	re := require.New(t)

	disableParanoidCheck, err := parseDisableParanoidCheck(map[string]any{})
	re.NoError(err)
	re.False(disableParanoidCheck)

	disableParanoidCheck, err = parseDisableParanoidCheck(map[string]any{"disable-paranoid-check": true})
	re.NoError(err)
	re.True(disableParanoidCheck)

	disableParanoidCheck, err = parseDisableParanoidCheck(map[string]any{"disable_paranoid_check": false})
	re.NoError(err)
	re.False(disableParanoidCheck)

	for _, input := range []map[string]any{
		{"disable-paranoid-check": "true"},
		{"disable_paranoid_check": 1},
	} {
		_, err = parseDisableParanoidCheck(input)
		re.Error(err)
	}

	_, err = parseDisableParanoidCheck(map[string]any{
		"disable-paranoid-check": true,
		"disable_paranoid_check": false,
	})
	re.EqualError(err, "disable-paranoid-check is specified multiple times")
}
