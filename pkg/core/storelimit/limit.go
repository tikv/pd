// Copyright 2022 TiKV Project Authors.
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

package storelimit

import (
	"github.com/tikv/pd/pkg/core/constant"
)

// Type indicates the type of store limit
type Type int

const (
	// AddPeer indicates the type of store limit that limits the adding peer rate
	AddPeer Type = iota
	// RemovePeer indicates the type of store limit that limits the removing peer rate
	RemovePeer
	// SendSnapshot indicates the type of sending snapshot.
	SendSnapshot

	storeLimitTypeLen
)

const (
	// VersionV1 represents the rate limit version of the store limit
	VersionV1 = "v1"
	// VersionV2 represents the sliding window version of the store limit
	VersionV2 = "v2"
)

// StoreLimit is an interface to control the operator rate of store
// TODO: add a method to control the rate of store
// the normal control flow is:
// 1. check the store limit with Available in checker or scheduler.
// 2. check the store limit with Available in operator controller again.
// the different between 1 and 2 is that 1 maybe not use the operator level.
// 3. take the cost of operator with Take in operator controller.
// 4. ack will put back the cost into the limit for the next waiting operator after the operator is finished.
// the cost is the operator influence, so the influence should be same in the life of the operator.
type StoreLimit interface {
	// Available returns true if the store can accept the operator
	Available(cost int64, typ Type, level constant.PriorityLevel) bool
	// Take takes the cost of the operator, it returns false if the store can't accept any operators.
	Take(count int64, typ Type, level constant.PriorityLevel) bool
	// Reset resets the store limit
	Reset(rate float64, typ Type)
	// Feedback update limit capacity by auto-tuning.
	Feedback(e float64)
	// Ack put back the cost into the limit for the next waiting operator after the operator is finished.
	// only snapshot type can use this method.
	Ack(cost int64, typ Type)
	// Version returns the version of the store limit
	Version() string
}
