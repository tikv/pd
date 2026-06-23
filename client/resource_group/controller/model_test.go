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
	expected := float64(6)

	result := getRUValueFromConsumption(custom)
	re.Equal(expected, result)

	// When custom is nil
	custom = nil
	expected = float64(0)

	result = getRUValueFromConsumption(custom)
	re.Equal(expected, result)
}

func TestAdd(t *testing.T) {
	// Positive test case
	re := require.New(t)
	custom1 := &rmpb.Consumption{RRU: 2.5, WRU: 3.5, ReadCrossAzTrafficBytes: 10, WriteCrossAzTrafficBytes: 20, TikvRUV2: 1, TidbRUV2: 2, TiflashRUV2: 3}
	custom2 := &rmpb.Consumption{RRU: 1.5, WRU: 2.5, ReadCrossAzTrafficBytes: 30, WriteCrossAzTrafficBytes: 40, TikvRUV2: 3, TidbRUV2: 4, TiflashRUV2: 5}
	expected := &rmpb.Consumption{
		RRU:                      4,
		WRU:                      6,
		ReadBytes:                0,
		WriteBytes:               0,
		TotalCpuTimeMs:           0,
		SqlLayerCpuTimeMs:        0,
		KvReadRpcCount:           0,
		KvWriteRpcCount:          0,
		ReadCrossAzTrafficBytes:  40,
		WriteCrossAzTrafficBytes: 60,
		TikvRUV2:                 4,
		TidbRUV2:                 6,
		TiflashRUV2:              8,
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
	custom1 := &rmpb.Consumption{RRU: 2.5, WRU: 3.5, ReadCrossAzTrafficBytes: 5, WriteCrossAzTrafficBytes: 10, TikvRUV2: 7, TidbRUV2: 9, TiflashRUV2: 11}
	custom2 := &rmpb.Consumption{RRU: 1.5, WRU: 2.5, ReadCrossAzTrafficBytes: 1, WriteCrossAzTrafficBytes: 2, TikvRUV2: 3, TidbRUV2: 4, TiflashRUV2: 5}
	expected := &rmpb.Consumption{
		RRU:                      1,
		WRU:                      1,
		ReadBytes:                0,
		WriteBytes:               0,
		TotalCpuTimeMs:           0,
		SqlLayerCpuTimeMs:        0,
		KvReadRpcCount:           0,
		KvWriteRpcCount:          0,
		ReadCrossAzTrafficBytes:  4,
		WriteCrossAzTrafficBytes: 8,
		TikvRUV2:                 4,
		TidbRUV2:                 5,
		TiflashRUV2:              6,
	}

	sub(custom1, custom2)
	re.Equal(expected, custom1)
	// When custom1 is nil
	custom1 = nil
	custom2 = &rmpb.Consumption{RRU: 1.5, WRU: 2.55, ReadCrossAzTrafficBytes: 1, WriteCrossAzTrafficBytes: 2}
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

func TestUpdateDeltaConsumption(t *testing.T) {
	re := require.New(t)
	last := &rmpb.Consumption{TikvRUV2: 2, TidbRUV2: 3, TiflashRUV2: 5}
	now := &rmpb.Consumption{TikvRUV2: 5, TidbRUV2: 11, TiflashRUV2: 18}

	delta := updateDeltaConsumption(last, now)

	re.Equal(&rmpb.Consumption{TikvRUV2: 3, TidbRUV2: 8, TiflashRUV2: 13}, delta)
	re.Equal(now.TikvRUV2, last.TikvRUV2)
	re.Equal(now.TidbRUV2, last.TidbRUV2)
	re.Equal(now.TiflashRUV2, last.TiflashRUV2)
}

func TestUpdateDeltaConsumptionReportsSignedRequestUnits(t *testing.T) {
	re := require.New(t)
	last := &rmpb.Consumption{
		RRU:                      10,
		WRU:                      20,
		ReadBytes:                100,
		WriteBytes:               200,
		TotalCpuTimeMs:           300,
		SqlLayerCpuTimeMs:        400,
		KvReadRpcCount:           500,
		KvWriteRpcCount:          600,
		ReadCrossAzTrafficBytes:  700,
		WriteCrossAzTrafficBytes: 800,
		TikvRUV2:                 900,
		TidbRUV2:                 1000,
		TiflashRUV2:              1100,
	}
	now := &rmpb.Consumption{
		RRU:                      7,
		WRU:                      15,
		ReadBytes:                90,
		WriteBytes:               190,
		TotalCpuTimeMs:           290,
		SqlLayerCpuTimeMs:        390,
		KvReadRpcCount:           490,
		KvWriteRpcCount:          590,
		ReadCrossAzTrafficBytes:  690,
		WriteCrossAzTrafficBytes: 790,
		TikvRUV2:                 890,
		TidbRUV2:                 990,
		TiflashRUV2:              1090,
	}

	delta := updateDeltaConsumption(last, now)

	re.Equal(float64(-3), delta.RRU)
	re.Equal(float64(-5), delta.WRU)
	re.Zero(delta.ReadBytes)
	re.Zero(delta.WriteBytes)
	re.Zero(delta.TotalCpuTimeMs)
	re.Zero(delta.SqlLayerCpuTimeMs)
	re.Zero(delta.KvReadRpcCount)
	re.Zero(delta.KvWriteRpcCount)
	re.Zero(delta.ReadCrossAzTrafficBytes)
	re.Zero(delta.WriteCrossAzTrafficBytes)
	re.Zero(delta.TikvRUV2)
	re.Zero(delta.TidbRUV2)
	re.Zero(delta.TiflashRUV2)
	re.Equal(now.RRU, last.RRU)
	re.Equal(now.WRU, last.WRU)
	re.Equal(float64(100), last.ReadBytes)
	re.Equal(float64(200), last.WriteBytes)
	re.Equal(float64(300), last.TotalCpuTimeMs)
	re.Equal(float64(400), last.SqlLayerCpuTimeMs)
	re.Equal(float64(500), last.KvReadRpcCount)
	re.Equal(float64(600), last.KvWriteRpcCount)
	re.Equal(uint64(700), last.ReadCrossAzTrafficBytes)
	re.Equal(uint64(800), last.WriteCrossAzTrafficBytes)
	re.Equal(float64(900), last.TikvRUV2)
	re.Equal(float64(1000), last.TidbRUV2)
	re.Equal(float64(1100), last.TiflashRUV2)
}

func TestEqualRU(t *testing.T) {
	re := require.New(t)

	re.True(equalRU(
		rmpb.Consumption{RRU: 1, WRU: 2, TikvRUV2: 3, TidbRUV2: 4},
		rmpb.Consumption{RRU: 1, WRU: 2, TikvRUV2: 3, TidbRUV2: 4},
	))
	re.False(equalRU(
		rmpb.Consumption{RRU: 1, WRU: 2, TikvRUV2: 3, TidbRUV2: 4},
		rmpb.Consumption{RRU: 1, WRU: 2, TikvRUV2: 5, TidbRUV2: 4},
	))
	re.False(equalRU(
		rmpb.Consumption{RRU: 1, WRU: 2, TikvRUV2: 3, TidbRUV2: 4},
		rmpb.Consumption{RRU: 1, WRU: 2, TikvRUV2: 3, TidbRUV2: 6},
	))
}
