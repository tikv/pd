// Copyright 2023 TiKV Project Authors.
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

package client

import (
	"context"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

// RequestUnit is the basic unit of the resource request management, which has two types:
//   - RRU: read request unit
//   - WRU: write request unit
type RequestUnit float64

// RequestInfo is the interface of the request information provider. A request should be
// able tell whether it's a write request and if so, the written bytes would also be provided.
type RequestInfo interface {
	IsWrite() bool
	WriteBytes() uint64
}

// ResponseInfo is the interface of the response information provider. A response should be
// able tell how many bytes it read and KV CPU cost in milliseconds.
type ResponseInfo interface {
	ReadBytes() uint64
	KVCPUMs() uint64
}

// ResourceCalculator is used to calculate the resource consumption of a request.
type ResourceCalculator interface {
	// Trickle is used to calculate the resource consumption periodically rather than on the request path.
	// It's mainly used to calculate like the SQL CPU cost.
	Trickle(context.Context, *rmpb.Consumption)
	// BeforeKVRequest is used to calculate the resource consumption before the KV request.
	// It's mainly used to calculate the base and write request cost.
	BeforeKVRequest(*rmpb.Consumption, RequestInfo)
	// AfterKVRequest is used to calculate the resource consumption after the KV request.
	// It's mainly used to calculate the read request cost and KV CPU cost.
	AfterKVRequest(*rmpb.Consumption, RequestInfo, ResponseInfo)
}

// KVCalculator is used to calculate the KV-side consumption.
type KVCalculator struct {
	*Config
}

var _ ResourceCalculator = (*KVCalculator)(nil)

func newKVCalculator(cfg *Config) *KVCalculator {
	return &KVCalculator{Config: cfg}
}

// Trickle ...
func (kc *KVCalculator) Trickle(ctx context.Context, consumption *rmpb.Consumption) {
}

// BeforeKVRequest ...
func (kc *KVCalculator) BeforeKVRequest(consumption *rmpb.Consumption, req RequestInfo) {
	if req.IsWrite() {
		consumption.KvWriteRpcCount += 1
		// Write bytes are knowable in advance, so we can calculate the WRU cost here.
		writeBytes := float64(req.WriteBytes())
		consumption.WriteBytes += writeBytes
		consumption.WRU += float64(kc.WriteBaseCost) + float64(kc.WriteBytesCost)*writeBytes
	} else {
		consumption.KvReadRpcCount += 1
		// Read bytes could not be known before the request is executed,
		// so we only add the base cost here.
		consumption.RRU += float64(kc.ReadBaseCost)
	}
}

// AfterKVRequest ...
func (kc *KVCalculator) AfterKVRequest(consumption *rmpb.Consumption, req RequestInfo, res ResponseInfo) {
	// For now, we can only collect the KV CPU cost for a read request.
	if !req.IsWrite() {
		kvCPUMs := float64(res.KVCPUMs())
		consumption.TotalCpuTimeMs += kvCPUMs
		consumption.RRU += float64(kc.CPUMsCost) * kvCPUMs
	}
	// A write request may also read data, which should be counted into the RRU cost.
	readBytes := float64(res.ReadBytes())
	consumption.ReadBytes += readBytes
	consumption.RRU += float64(kc.ReadBytesCost) * readBytes
}

// SQLCalculator is used to calculate the SQL-side consumption.
type SQLCalculator struct {
	*Config
}

var _ ResourceCalculator = (*SQLCalculator)(nil)

func newSQLCalculator(cfg *Config) *SQLCalculator {
	return &SQLCalculator{Config: cfg}
}

// Trickle ...
// TODO: calculate the SQL CPU cost and related resource consumption.
func (dsc *SQLCalculator) Trickle(ctx context.Context, consumption *rmpb.Consumption) {
}

// BeforeKVRequest ...
func (dsc *SQLCalculator) BeforeKVRequest(consumption *rmpb.Consumption, req RequestInfo) {
}

// AfterKVRequest ...
func (dsc *SQLCalculator) AfterKVRequest(consumption *rmpb.Consumption, req RequestInfo, res ResponseInfo) {
}

func getRUValueFromConsumption(custom *rmpb.Consumption, typ rmpb.RequestUnitType) float64 {
	if typ == 0 {
		return custom.RRU + custom.WRU
	}
	return 0
}

func getRUTokenBucketSetting(group *rmpb.ResourceGroup, typ rmpb.RequestUnitType) *rmpb.TokenBucket {
	if typ == 0 {
		return group.RUSettings.RU
	}
	return nil
}

func getRawResourceValueFromConsumption(custom *rmpb.Consumption, typ rmpb.RawResourceType) float64 {
	switch typ {
	case 0:
		return custom.TotalCpuTimeMs
	case 1:
		return custom.ReadBytes
	case 2:
		return custom.WriteBytes
	}
	return 0
}

func getRawResourceTokenBucketSetting(group *rmpb.ResourceGroup, typ rmpb.RawResourceType) *rmpb.TokenBucket {
	switch typ {
	case 0:
		return group.RawResourceSettings.Cpu
	case 1:
		return group.RawResourceSettings.IoRead
	case 2:
		return group.RawResourceSettings.IoWrite
	}
	return nil
}

func add(custom1 *rmpb.Consumption, custom2 *rmpb.Consumption) {
	custom1.RRU += custom2.RRU
	custom1.WRU += custom2.WRU
	custom1.ReadBytes += custom2.ReadBytes
	custom1.WriteBytes += custom2.WriteBytes
	custom1.TotalCpuTimeMs += custom2.TotalCpuTimeMs
	custom1.SqlLayerCpuTimeMs += custom2.SqlLayerCpuTimeMs
	custom1.KvReadRpcCount += custom2.KvReadRpcCount
	custom1.KvWriteRpcCount += custom2.KvWriteRpcCount
}

func sub(custom1 *rmpb.Consumption, custom2 *rmpb.Consumption) {
	custom1.RRU -= custom2.RRU
	custom1.WRU -= custom2.WRU
	custom1.ReadBytes -= custom2.ReadBytes
	custom1.WriteBytes -= custom2.WriteBytes
	custom1.TotalCpuTimeMs -= custom2.TotalCpuTimeMs
	custom1.SqlLayerCpuTimeMs -= custom2.SqlLayerCpuTimeMs
	custom1.KvReadRpcCount -= custom2.KvReadRpcCount
	custom1.KvWriteRpcCount -= custom2.KvWriteRpcCount
}
