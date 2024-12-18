// Copyright 2023 TiKV Project Authors.
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

package controller

import (
	"testing"

	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

func TestGetRUValueFromConsumption(t *testing.T) {
	// Positive test case
	re := require.New(t)
	custom := &rmpb.Consumption{RRU: 2.5, WRU: 3.5}
	typ := rmpb.RequestUnitType_RU
	expected := float64(6)

	result := getRUValueFromConsumption(custom, typ)
	re.Equal(expected, result)

	// When custom is nil
	custom = nil
	expected = float64(0)

	result = getRUValueFromConsumption(custom, typ)
	re.Equal(expected, result)
}

func TestGetRUTokenBucketSetting(t *testing.T) {
	// Positive test case
	re := require.New(t)
	group := &rmpb.ResourceGroup{
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 100}},
		},
	}
	typ := rmpb.RequestUnitType_RU
	expected := &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 100}}

	result := getRUTokenBucketSetting(group, typ)
	re.Equal(expected.GetSettings().GetFillRate(), result.GetSettings().GetFillRate())

	// When group is nil
	group = nil
	expected = nil

	result = getRUTokenBucketSetting(group, typ)
	if result != expected {
		t.Errorf("Expected nil but got %v", result)
	}
}

func TestGetRawResourceValueFromConsumption(t *testing.T) {
	// Positive test case
	re := require.New(t)
	custom := &rmpb.Consumption{TotalCpuTimeMs: 50}
	typ := rmpb.RawResourceType_CPU
	expected := float64(50)

	result := getRawResourceValueFromConsumption(custom, typ)
	re.Equal(expected, result)

	// When custom is nil
	custom = nil
	expected = float64(0)

	result = getRawResourceValueFromConsumption(custom, typ)
	re.Equal(expected, result)

	// When typ is IOReadFlow
	custom = &rmpb.Consumption{ReadBytes: 200}
	typ = rmpb.RawResourceType_IOReadFlow
	expected = float64(200)

	result = getRawResourceValueFromConsumption(custom, typ)
	re.Equal(expected, result)
}

func TestGetRawResourceTokenBucketSetting(t *testing.T) {
	// Positive test case
	re := require.New(t)
	group := &rmpb.ResourceGroup{
		RawResourceSettings: &rmpb.GroupRawResourceSettings{
			Cpu: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 100}},
		},
	}
	typ := rmpb.RawResourceType_CPU
	expected := &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 100}}

	result := getRawResourceTokenBucketSetting(group, typ)

	re.Equal(expected.GetSettings().GetFillRate(), result.GetSettings().GetFillRate())

	// When group is nil
	group = nil
	expected = nil

	result = getRawResourceTokenBucketSetting(group, typ)
	if result != expected {
		t.Errorf("Expected nil but got %v", result)
	}

	// When typ is IOReadFlow
	group = &rmpb.ResourceGroup{
		RawResourceSettings: &rmpb.GroupRawResourceSettings{
			IoRead: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 200}},
		},
	}
	typ = rmpb.RawResourceType_IOReadFlow
	expected = &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 200}}

	result = getRawResourceTokenBucketSetting(group, typ)
	re.Equal(expected.GetSettings().GetFillRate(), result.GetSettings().GetFillRate())
}

func TestAdd(t *testing.T) {
	// Positive test case
	re := require.New(t)
	custom1 := &rmpb.Consumption{RRU: 2.5, WRU: 3.5}
	custom2 := &rmpb.Consumption{RRU: 1.5, WRU: 2.5}
	expected := &rmpb.Consumption{
		RRU:               4,
		WRU:               6,
		ReadBytes:         0,
		WriteBytes:        0,
		TotalCpuTimeMs:    0,
		SqlLayerCpuTimeMs: 0,
		KvReadRpcCount:    0,
		KvWriteRpcCount:   0,
	}

	add(custom1, custom2)
	re.Equal(expected, custom1)

	// When custom1 is nil
	custom1 = nil
	custom2 = &rmpb.Consumption{RRU: 1.5, WRU: 2.5}
	expected = nil

	add(custom1, custom2)
	re.Equal(expected, custom1)

	// When custom2 is nil
	custom1 = &rmpb.Consumption{RRU: 2.5, WRU: 3.5}
	custom2 = nil
	expected = &rmpb.Consumption{RRU: 2.5, WRU: 3.5}

	add(custom1, custom2)
	re.Equal(expected, custom1)
}

func TestSub(t *testing.T) {
	// Positive test case
	re := require.New(t)
	custom1 := &rmpb.Consumption{RRU: 2.5, WRU: 3.5}
	custom2 := &rmpb.Consumption{RRU: 1.5, WRU: 2.5}
	expected := &rmpb.Consumption{
		RRU:               1,
		WRU:               1,
		ReadBytes:         0,
		WriteBytes:        0,
		TotalCpuTimeMs:    0,
		SqlLayerCpuTimeMs: 0,
		KvReadRpcCount:    0,
		KvWriteRpcCount:   0,
	}

	sub(custom1, custom2)
	re.Equal(expected, custom1)
	// When custom1 is nil
	custom1 = nil
	custom2 = &rmpb.Consumption{RRU: 1.5, WRU: 2.5}
	expected = nil

	sub(custom1, custom2)
	re.Equal(expected, custom1)

	// When custom2 is nil
	custom1 = &rmpb.Consumption{RRU: 2.5, WRU: 3.5}
	custom2 = nil
	expected = &rmpb.Consumption{RRU: 2.5, WRU: 3.5}

	sub(custom1, custom2)
	re.Equal(expected, custom1)
}
