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

package operator

import (
	"time"
)

// SetOperatorStatusReachTime copies the operator and sets the reach time of the copy.
// NOTE: Shoud only use in test.
func SetOperatorStatusReachTime(op *Operator, st OpStatus, t time.Time) {
	op.status.setTime(st, t)
}

// SetOperatorCurrentStatus copies the operator and sets the status of copy to the given status.
// NOTE: Should only use in test.
func SetOperatorCurrentStatus(op *Operator, st OpStatus) {
	op.status.current = st
}
