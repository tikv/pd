// Copyright 2019 PingCAP, Inc.
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

import "time"

const (
	physicalShiftBits = 18
	logicalBits       = 0x3FFFF
)

// ParseTS parses the ts to (pythsic,logical).
func ParseTS(ts int64) (time.Time, int64) {
	logical := ts & logicalBits
	pythsic := ts >> physicalShiftBits
	pythsicalTime := time.Unix(pythsic/1000, int64(pythsic%1000)*time.Millisecond.Nanoseconds())
	return pythsicalTime, logical
}
