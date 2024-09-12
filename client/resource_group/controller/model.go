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

package controller

import (
	"os"
	"time"

	sigar "github.com/cloudfoundry/gosigar"
	"go.uber.org/zap"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
)

// RequestUnit is the basic unit of the resource request management, which has two types:
//   - RRU: read request unit
//   - WRU: write request unit
type RequestUnit float64

// RequestInfo is the interface of the request information provider. A request should be
// able to tell whether it's a write request and if so, the written bytes would also be provided.
type RequestInfo interface {
	IsWrite() bool
	WriteBytes() uint64
	ReplicaNumber() int64
	StoreID() uint64
}

// ResponseInfo is the interface of the response information provider. A response should be
// able to tell how many bytes it read and KV CPU cost in milliseconds.
type ResponseInfo interface {
	ReadBytes() uint64
	KVCPU() time.Duration
	// Succeed is used to tell whether the request is successfully returned.
	// If not, we need to pay back the WRU cost of the request.
	Succeed() bool
}

// ResourceCalculator is used to calculate the resource consumption of a request.
type ResourceCalculator interface {
	// Trickle is used to calculate the resource consumption periodically rather than on the request path.
	// It's mainly used to calculate like the SQL CPU cost.
	// Need to check if it is a serverless environment
	Trickle(*rmpb.Consumption)
	// BeforeKVRequest is used to calculate the resource consumption before the KV request.
	// It's mainly used to calculate the base and write request cost.
	BeforeKVRequest(*rmpb.Consumption, RequestInfo)
	// AfterKVRequest is used to calculate the resource consumption after the KV request.
	// It's mainly used to calculate the read request cost and KV CPU cost.
	AfterKVRequest(*rmpb.Consumption, RequestInfo, ResponseInfo)
}

// KVCalculator is used to calculate the KV-side consumption.
type KVCalculator struct {
	*RUConfig
}

var _ ResourceCalculator = (*KVCalculator)(nil)

func newKVCalculator(cfg *RUConfig) *KVCalculator {
	return &KVCalculator{RUConfig: cfg}
}

// Trickle ...
func (*KVCalculator) Trickle(*rmpb.Consumption) {}

// BeforeKVRequest ...
func (kc *KVCalculator) BeforeKVRequest(consumption *rmpb.Consumption, req RequestInfo) {
	if req.IsWrite() {
		consumption.KvWriteRpcCount += 1
		// Write bytes are knowable in advance, so we can calculate the WRU cost here.
		kc.calculateWriteCost(consumption, req)
	} else {
		consumption.KvReadRpcCount += 1
		// Read bytes could not be known before the request is executed,
		// so we only add the base cost here.
		consumption.RRU += float64(kc.ReadBaseCost) + float64(kc.ReadPerBatchBaseCost)*defaultAvgBatchProportion
	}
}

func (kc *KVCalculator) calculateWriteCost(consumption *rmpb.Consumption, req RequestInfo) {
	writeBytes := float64(req.WriteBytes())
	consumption.WriteBytes += writeBytes
	// write request cost need consider the replicas, due to write data will be replicate to all replicas.
	replicaNums := float64(req.ReplicaNumber())
	if replicaNums == 0 {
		replicaNums = 1
	}
	consumption.WRU += (float64(kc.WriteBaseCost) + float64(kc.WritePerBatchBaseCost)*defaultAvgBatchProportion + float64(kc.WriteBytesCost)*writeBytes) * replicaNums
}

// AfterKVRequest ...
func (kc *KVCalculator) AfterKVRequest(consumption *rmpb.Consumption, req RequestInfo, res ResponseInfo) {
	if !req.IsWrite() {
		// For now, we can only collect the KV CPU cost for a read request.
		kc.calculateCPUCost(consumption, res)
	} else if !res.Succeed() {
		// If the write request is not successfully returned, we need to pay back the WRU cost.
		kc.payBackWriteCost(consumption, req)
	}
	// A write request may also read data, which should be counted into the RRU cost.
	// This part should be counted even if the request does not succeed.
	kc.calculateReadCost(consumption, res)
}

func (kc *KVCalculator) calculateReadCost(consumption *rmpb.Consumption, res ResponseInfo) {
	if consumption == nil {
		return
	}
	readBytes := float64(res.ReadBytes())
	consumption.ReadBytes += readBytes
	consumption.RRU += float64(kc.ReadBytesCost) * readBytes
}

func (kc *KVCalculator) calculateCPUCost(consumption *rmpb.Consumption, res ResponseInfo) {
	if consumption == nil {
		return
	}
	kvCPUMs := float64(res.KVCPU().Nanoseconds()) / 1000000.0
	consumption.TotalCpuTimeMs += kvCPUMs
	consumption.RRU += float64(kc.CPUMsCost) * kvCPUMs
}

func (kc *KVCalculator) payBackWriteCost(consumption *rmpb.Consumption, req RequestInfo) {
	if consumption == nil {
		return
	}
	writeBytes := float64(req.WriteBytes())
	consumption.WriteBytes -= writeBytes
	consumption.WRU -= float64(kc.WriteBaseCost) + float64(kc.WriteBytesCost)*writeBytes
}

// SQLCalculator is used to calculate the SQL-side consumption.
type SQLCalculator struct {
	*RUConfig
}

var _ ResourceCalculator = (*SQLCalculator)(nil)

func newSQLCalculator(cfg *RUConfig) *SQLCalculator {
	return &SQLCalculator{RUConfig: cfg}
}

// Trickle update sql layer CPU consumption.
func (dsc *SQLCalculator) Trickle(consumption *rmpb.Consumption) {
	if consumption == nil {
		return
	}
	delta := getSQLProcessCPUTime(dsc.isSingleGroupByKeyspace) - consumption.SqlLayerCpuTimeMs
	consumption.TotalCpuTimeMs += delta
	consumption.SqlLayerCpuTimeMs += delta
}

// BeforeKVRequest ...
func (*SQLCalculator) BeforeKVRequest(*rmpb.Consumption, RequestInfo) {
}

// AfterKVRequest ...
func (*SQLCalculator) AfterKVRequest(*rmpb.Consumption, RequestInfo, ResponseInfo) {
}

func getRUValueFromConsumption(custom *rmpb.Consumption, typ rmpb.RequestUnitType) float64 {
	if custom == nil {
		return 0
	}
	if typ == rmpb.RequestUnitType_RU {
		return custom.RRU + custom.WRU
	}
	return 0
}

func getRUTokenBucketSetting(group *rmpb.ResourceGroup, typ rmpb.RequestUnitType) *rmpb.TokenBucket {
	if group == nil {
		return nil
	}
	if typ == rmpb.RequestUnitType_RU {
		return group.RUSettings.RU
	}
	return nil
}

func getRawResourceValueFromConsumption(custom *rmpb.Consumption, typ rmpb.RawResourceType) float64 {
	if custom == nil {
		return 0
	}
	switch typ {
	case rmpb.RawResourceType_CPU:
		return custom.TotalCpuTimeMs
	case rmpb.RawResourceType_IOReadFlow:
		return custom.ReadBytes
	case rmpb.RawResourceType_IOWriteFlow:
		return custom.WriteBytes
	}
	return 0
}

func getRawResourceTokenBucketSetting(group *rmpb.ResourceGroup, typ rmpb.RawResourceType) *rmpb.TokenBucket {
	if group == nil {
		return nil
	}
	switch typ {
	case rmpb.RawResourceType_CPU:
		return group.RawResourceSettings.Cpu
	case rmpb.RawResourceType_IOReadFlow:
		return group.RawResourceSettings.IoRead
	case rmpb.RawResourceType_IOWriteFlow:
		return group.RawResourceSettings.IoWrite
	}
	return nil
}

func add(custom1 *rmpb.Consumption, custom2 *rmpb.Consumption) {
	if custom1 == nil || custom2 == nil {
		return
	}
	custom1.RRU += custom2.RRU
	custom1.WRU += custom2.WRU
	custom1.ReadBytes += custom2.ReadBytes
	custom1.WriteBytes += custom2.WriteBytes
	custom1.TotalCpuTimeMs += custom2.TotalCpuTimeMs
	custom1.SqlLayerCpuTimeMs += custom2.SqlLayerCpuTimeMs
	custom1.KvReadRpcCount += custom2.KvReadRpcCount
	custom1.KvWriteRpcCount += custom2.KvWriteRpcCount
}

func updateDeltaConsumption(last *rmpb.Consumption, now *rmpb.Consumption) *rmpb.Consumption {
	delta := &rmpb.Consumption{}
	if now.RRU >= last.RRU {
		delta.RRU = now.RRU - last.RRU
		last.RRU = now.RRU
	}
	if now.WRU >= last.WRU {
		delta.WRU = now.WRU - last.WRU
		last.WRU = now.WRU
	}
	if now.ReadBytes >= last.ReadBytes {
		delta.ReadBytes = now.ReadBytes - last.ReadBytes
		last.ReadBytes = now.ReadBytes
	}
	if now.WriteBytes >= last.WriteBytes {
		delta.WriteBytes = now.WriteBytes - last.WriteBytes
		last.WriteBytes = now.WriteBytes
	}
	if now.TotalCpuTimeMs >= last.TotalCpuTimeMs {
		delta.TotalCpuTimeMs = now.TotalCpuTimeMs - last.TotalCpuTimeMs
		last.TotalCpuTimeMs = now.TotalCpuTimeMs
	}
	if now.SqlLayerCpuTimeMs >= last.SqlLayerCpuTimeMs {
		delta.SqlLayerCpuTimeMs = now.SqlLayerCpuTimeMs - last.SqlLayerCpuTimeMs
		last.SqlLayerCpuTimeMs = now.SqlLayerCpuTimeMs
	}
	if now.KvReadRpcCount >= last.KvReadRpcCount {
		delta.KvReadRpcCount = now.KvReadRpcCount - last.KvReadRpcCount
		last.KvReadRpcCount = now.KvReadRpcCount
	}
	if now.KvWriteRpcCount >= last.KvWriteRpcCount {
		delta.KvWriteRpcCount = now.KvWriteRpcCount - last.KvWriteRpcCount
		last.KvWriteRpcCount = now.KvWriteRpcCount
	}
	return delta
}

func sub(custom1 *rmpb.Consumption, custom2 *rmpb.Consumption) {
	if custom1 == nil || custom2 == nil {
		return
	}
	custom1.RRU -= custom2.RRU
	custom1.WRU -= custom2.WRU
	custom1.ReadBytes -= custom2.ReadBytes
	custom1.WriteBytes -= custom2.WriteBytes
	custom1.TotalCpuTimeMs -= custom2.TotalCpuTimeMs
	custom1.SqlLayerCpuTimeMs -= custom2.SqlLayerCpuTimeMs
	custom1.KvReadRpcCount -= custom2.KvReadRpcCount
	custom1.KvWriteRpcCount -= custom2.KvWriteRpcCount
}

func equalRU(custom1 rmpb.Consumption, custom2 rmpb.Consumption) bool {
	return custom1.RRU == custom2.RRU && custom1.WRU == custom2.WRU
}

// getSQLProcessCPUTime returns the cumulative user+system time (in ms) since the process start.
func getSQLProcessCPUTime(isSingleGroupByKeyspace bool) float64 {
	if isSingleGroupByKeyspace {
		return getSysProcessCPUTime()
	}
	return getGroupProcessCPUTime()
}

func getSysProcessCPUTime() float64 {
	pid := os.Getpid()
	cpuTime := sigar.ProcTime{}
	if err := cpuTime.Get(pid); err != nil {
		log.Error("getCPUTime get pid failed", zap.Error(err))
	}

	return float64(cpuTime.User + cpuTime.Sys)
}

// TODO: Need a way to calculate in the case of multiple groups.
func getGroupProcessCPUTime() float64 {
	return 0
}
