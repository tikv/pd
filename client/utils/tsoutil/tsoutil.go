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

package tsoutil

import (
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// TSLessEqual returns true if (physical, logical) <= (thatPhysical, thatLogical).
func TSLessEqual(physical, logical, thatPhysical, thatLogical int64) bool {
	if physical == thatPhysical {
		return logical <= thatLogical
	}
	return physical < thatPhysical
}

// CompareTimestamp is used to compare two timestamps.
// If tsoOne > tsoTwo, returns 1.
// If tsoOne = tsoTwo, returns 0.
// If tsoOne < tsoTwo, returns -1.
func CompareTimestamp(tsoOne, tsoTwo *pdpb.Timestamp) int {
	if tsoOne.GetPhysical() > tsoTwo.GetPhysical() || (tsoOne.GetPhysical() == tsoTwo.GetPhysical() && tsoOne.GetLogical() > tsoTwo.GetLogical()) {
		return 1
	}
	if tsoOne.GetPhysical() == tsoTwo.GetPhysical() && tsoOne.GetLogical() == tsoTwo.GetLogical() {
		return 0
	}
	return -1
}
