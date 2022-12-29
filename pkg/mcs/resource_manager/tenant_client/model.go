// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,g
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tenantclient

import (
	"context"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

type RequestUnit float64

type RequestInfo interface {
	IsWrite() bool
	WriteBytes() uint64
}

type ResponseInfo interface {
	ReadBytes() uint64
	KVCPUms() uint64
}

func Sub(c float64, other float64) float64 {
	if c < other {
		return 0
	} else {
		return c - other
	}
}

type ResourceCalculator interface {
	Trickle(map[rmpb.ResourceType]float64, map[rmpb.RequestUnitType]float64, context.Context)
	BeforeKVRequest(map[rmpb.ResourceType]float64, map[rmpb.RequestUnitType]float64, RequestInfo)
	AfterKVRequest(map[rmpb.ResourceType]float64, map[rmpb.RequestUnitType]float64, RequestInfo, ResponseInfo)
}

type KVCalculator struct {
	*Config
}

func newKVCalculator(cfg *Config) *KVCalculator {
	return &KVCalculator{Config: cfg}
}

func (dwc *KVCalculator) Trickle(resource map[rmpb.ResourceType]float64, ru map[rmpb.RequestUnitType]float64, ctx context.Context) {
}

func (dwc *KVCalculator) BeforeKVRequest(resource map[rmpb.ResourceType]float64, ru map[rmpb.RequestUnitType]float64, req RequestInfo) {
	if req.IsWrite() {
		resource[rmpb.ResourceType_KVWriteRPCCount] += 1

		writeBytes := req.WriteBytes()
		resource[rmpb.ResourceType_WriteBytes] += float64(writeBytes)

		ru[rmpb.RequestUnitType_WRU] += float64(dwc.WriteRequestCost)
		ru[rmpb.RequestUnitType_WRU] += float64(dwc.WriteBytesCost) * float64(writeBytes)
	} else {
		resource[rmpb.ResourceType_KVReadRPCCount] += 1
		ru[rmpb.RequestUnitType_RRU] += float64(dwc.ReadRequestCost)
	}
}
func (dwc *KVCalculator) AfterKVRequest(resource map[rmpb.ResourceType]float64, ru map[rmpb.RequestUnitType]float64, req RequestInfo, res ResponseInfo) {
	readBytes := res.ReadBytes()
	resource[rmpb.ResourceType_ReadBytes] += float64(readBytes)

	ru[rmpb.RequestUnitType_RRU] += float64(readBytes) * float64(dwc.ReadBytesCost)

	kvCPUms := float64(res.KVCPUms())
	resource[rmpb.ResourceType_TotalCPUTimeMs] += kvCPUms
	if req.IsWrite() {
		ru[rmpb.RequestUnitType_WRU] += kvCPUms * float64(dwc.WriteCPUMsCost)
	} else {
		ru[rmpb.RequestUnitType_RRU] += kvCPUms * float64(dwc.ReadCPUMsCost)
	}
}

type SQLLayerCPUCalculateor struct {
	*Config
}

func newSQLLayerCPUCalculateor(cfg *Config) *SQLLayerCPUCalculateor {
	return &SQLLayerCPUCalculateor{Config: cfg}
}

func (dsc *SQLLayerCPUCalculateor) Trickle(resource map[rmpb.ResourceType]float64, ru map[rmpb.RequestUnitType]float64, ctx context.Context) {
	// TODO: SQL Layer RU/resource custom
	cpuFunc := func(ctx context.Context) float64 {
		return 0.
	}
	cpu := cpuFunc(ctx)
	resource[rmpb.ResourceType_TotalCPUTimeMs] += cpu
	resource[rmpb.ResourceType_SQLLayerCPUTimeMs] += cpu
}

func (dsc *SQLLayerCPUCalculateor) BeforeKVRequest(resource map[rmpb.ResourceType]float64, ru map[rmpb.RequestUnitType]float64, req RequestInfo) {
}
func (dsc *SQLLayerCPUCalculateor) AfterKVRequest(resource map[rmpb.ResourceType]float64, ru map[rmpb.RequestUnitType]float64, req RequestInfo, res ResponseInfo) {
}
