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

// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package controller

import "time"

// TestRequestInfo is used to test the request info interface.
type TestRequestInfo struct {
	isWrite     bool
	writeBytes  uint64
	numReplicas int64
	storeID     uint64
	accessType  AccessLocationType
}

// NewTestRequestInfo creates a new TestRequestInfo.
func NewTestRequestInfo(isWrite bool, writeBytes uint64, storeID uint64, locationType AccessLocationType) *TestRequestInfo {
	return &TestRequestInfo{
		isWrite:     isWrite,
		writeBytes:  writeBytes,
		numReplicas: 1,
		storeID:     storeID,
		accessType:  locationType,
	}
}

// IsWrite implements the RequestInfo interface.
func (tri *TestRequestInfo) IsWrite() bool {
	return tri.isWrite
}

// WriteBytes implements the RequestInfo interface.
func (tri *TestRequestInfo) WriteBytes() uint64 {
	return tri.writeBytes
}

// StoreID implements the RequestInfo interface.
func (tri *TestRequestInfo) StoreID() uint64 {
	return tri.storeID
}

// ReplicaNumber implements the RequestInfo interface.
func (tri *TestRequestInfo) ReplicaNumber() int64 {
	return tri.numReplicas
}

// RequestSize implements the RequestSize interface.
func (tri *TestRequestInfo) RequestSize() uint64 {
	return tri.writeBytes
}

// AccessLocationType implements the AccessLocationType interface.
func (tri *TestRequestInfo) AccessLocationType() AccessLocationType {
	return tri.accessType
}

// TestResponseInfo is used to test the response info interface.
type TestResponseInfo struct {
	readBytes uint64
	kvCPU     time.Duration
	succeed   bool
}

// NewTestResponseInfo creates a new TestResponseInfo.
func NewTestResponseInfo(readBytes uint64, kvCPU time.Duration, succeed bool) *TestResponseInfo {
	return &TestResponseInfo{
		readBytes: readBytes,
		kvCPU:     kvCPU,
		succeed:   succeed,
	}
}

// ReadBytes implements the ResponseInfo interface.
func (tri *TestResponseInfo) ReadBytes() uint64 {
	return tri.readBytes
}

// KVCPU implements the ResponseInfo interface.
func (tri *TestResponseInfo) KVCPU() time.Duration {
	return tri.kvCPU
}

// Succeed implements the ResponseInfo interface.
func (tri *TestResponseInfo) Succeed() bool {
	return tri.succeed
}

// ResponseSize implements the ResponseSize interface.
func (tri *TestResponseInfo) ResponseSize() uint64 {
	return tri.readBytes
}
