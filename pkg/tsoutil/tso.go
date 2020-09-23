// Copyright 2019 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tsoutil

import (
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

const (
	physicalShiftBits = 18
	logicalBits       = (1 << physicalShiftBits) - 1
)

// ParseTS parses the ts to (physical,logical).
func ParseTS(ts uint64) (time.Time, uint64) {
	logical := ts & logicalBits
	physical := ts >> physicalShiftBits
	physicalTime := time.Unix(int64(physical/1000), int64(physical)%1000*time.Millisecond.Nanoseconds())
	return physicalTime, logical
}

// ParseTimestamp parses `pdpb.Timestamp` to `time.Time`
func ParseTimestamp(ts pdpb.Timestamp) (time.Time, uint64) {
	logical := uint64(ts.GetLogical())
	physicalTime := time.Unix(ts.GetPhysical()/1000, ts.GetPhysical()%1000*time.Millisecond.Nanoseconds())
	return physicalTime, logical
}

// GenerateTS generate an `uint64` TS by passing a `pdpb.Timestamp`.
func GenerateTS(ts *pdpb.Timestamp) uint64 {
	return uint64(ts.GetPhysical())<<18 | uint64(ts.GetLogical())&0x3FFFF
}

// GenerateTimestamp generate a `pdpb.Timestamp` by passing `time.Time` and `uint64`
func GenerateTimestamp(physical time.Time, logical uint64) *pdpb.Timestamp {
	return &pdpb.Timestamp{
		Physical: physical.UnixNano() / int64(time.Millisecond),
		Logical:  int64(logical),
	}
}
