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

type legacyRequestInfo struct {
	isWrite bool
}

func (req *legacyRequestInfo) IsWrite() bool {
	return req.isWrite
}

func (*legacyRequestInfo) WriteBytes() uint64 {
	return 0
}

func (*legacyRequestInfo) ReplicaNumber() int64 {
	return 0
}

func (*legacyRequestInfo) StoreID() uint64 {
	return 0
}

func (*legacyRequestInfo) RequestSize() uint64 {
	return 0
}

func (*legacyRequestInfo) AccessLocationType() AccessLocationType {
	return AccessUnknown
}

type copRequestInfoWithoutPrediction struct {
	legacyRequestInfo
}

func (*copRequestInfoWithoutPrediction) IsCop() bool {
	return true
}

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

func TestUpdateDeltaConsumptionIgnoresDecreasedRequestUnits(t *testing.T) {
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

	re.Nil(delta)
	re.Equal(float64(10), last.RRU)
	re.Equal(float64(20), last.WRU)
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

func TestUpdateDeltaSettlementReportsSignedRequestUnits(t *testing.T) {
	re := require.New(t)
	last := &rmpb.Consumption{RRU: 10, WRU: 20, ReadBytes: 100}
	now := &rmpb.Consumption{RRU: 7, WRU: 15, ReadBytes: 90}

	delta := updateDeltaSettlement(last, now)

	re.Equal(float64(-3), delta.RRU)
	re.Equal(float64(-5), delta.WRU)
	re.Zero(delta.ReadBytes)
	re.Equal(now.RRU, last.RRU)
	re.Equal(now.WRU, last.WRU)
	re.Equal(float64(100), last.ReadBytes)
}

func TestRequestInfoPagingProvidersAreOptional(t *testing.T) {
	re := require.New(t)
	cfg := DefaultRUConfig()
	kvCalc := newKVCalculator(cfg)
	req := &legacyRequestInfo{}

	bytesForEst, ok := pagingReadEstimate(req)
	re.False(ok)
	re.Zero(bytesForEst)

	consumption := &rmpb.Consumption{}
	kvCalc.BeforeKVRequest(consumption, req)
	re.InDelta(float64(cfg.ReadBaseCost)+float64(cfg.ReadPerBatchBaseCost)*defaultAvgBatchProportion, consumption.RRU, 1e-6)

	resp := &TestResponseInfo{readBytes: 1024, succeed: true}
	settle := &rmpb.Consumption{}
	kvCalc.AfterKVRequest(settle, req, resp)
	re.InDelta(float64(cfg.ReadBytesCost)*1024, settle.RRU, 1e-6)
}

func TestRequestInfoMissingPredictionProviderReturnsZeroHint(t *testing.T) {
	re := require.New(t)
	req := &copRequestInfoWithoutPrediction{}

	bytesForEst, ok := pagingReadEstimate(req)

	re.True(ok)
	re.Zero(bytesForEst)
}

func TestReportedConsumptionStripsPagingPrecharge(t *testing.T) {
	re := require.New(t)
	cfg := DefaultRUConfig()
	kvCalc := newKVCalculator(cfg)
	calculators := []ResourceCalculator{kvCalc}
	req := &TestRequestInfo{
		isWrite:            false,
		isCop:              true,
		predictedReadBytes: 1024,
	}

	tokenDelta := &rmpb.Consumption{
		RRU: float64(cfg.ReadBaseCost) +
			float64(cfg.ReadPerBatchBaseCost)*defaultAvgBatchProportion +
			float64(cfg.ReadBytesCost)*1024,
	}
	reported := reportedRequestConsumption(calculators, req, tokenDelta)

	re.InDelta(float64(cfg.ReadBaseCost)+
		float64(cfg.ReadPerBatchBaseCost)*defaultAvgBatchProportion,
		reported.RRU, 1e-6)
	re.InDelta(tokenDelta.RRU, reported.RRU+float64(cfg.ReadBytesCost)*1024, 1e-6)
}

func TestReportedConsumptionRestoresPagingSettlement(t *testing.T) {
	re := require.New(t)
	cfg := DefaultRUConfig()
	kvCalc := newKVCalculator(cfg)
	calculators := []ResourceCalculator{kvCalc}
	req := &TestRequestInfo{
		isWrite:            false,
		isCop:              true,
		predictedReadBytes: 4096,
	}

	actualReadBytes := uint64(1024)
	tokenDelta := &rmpb.Consumption{
		RRU:       float64(cfg.ReadBytesCost) * (float64(actualReadBytes) - float64(req.predictedReadBytes)),
		ReadBytes: float64(actualReadBytes),
	}
	reported := reportedResponseConsumption(calculators, req, &TestResponseInfo{succeed: true}, tokenDelta)

	re.InDelta(float64(cfg.ReadBytesCost)*float64(actualReadBytes), reported.RRU, 1e-6)
	re.Equal(float64(actualReadBytes), reported.ReadBytes)
	re.Negative(tokenDelta.RRU)
	re.GreaterOrEqual(reported.RRU, 0.0)
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
