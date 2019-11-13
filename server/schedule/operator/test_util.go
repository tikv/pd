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

	"github.com/pingcap/kvproto/pkg/metapb"
)

// CopyOperator returns a copy the operator.
func CopyOperator(op *Operator) *Operator {
	cp := &Operator{
		desc:     op.desc,
		brief:    op.brief,
		regionID: op.regionID,
		// regionEpoch
		kind: op.kind,
		// steps
		currentStep: op.currentStep,
		// status
		stepTime: op.stepTime,
		level:    op.level,
	}

	cp.regionEpoch = &metapb.RegionEpoch{
		ConfVer: op.regionEpoch.ConfVer,
		Version: op.regionEpoch.Version,
	}

	cp.steps = make([]OpStep, len(op.steps))
	// FIXME: deep copy step
	copy(cp.steps, op.steps)

	cp.status = NewOpStatusTracker()
	cp.status.current = op.status.current
	copy(cp.status.reachTimes[:], op.status.reachTimes[:])

	return cp
}

// SetOperatorStatusReachTime copies the operator and sets the reach time of the copy.
// NOTE: Shoud only use in test.
func SetOperatorStatusReachTime(op *Operator, st OpStatus, t time.Time) *Operator {
	cp := CopyOperator(op)
	cp.status.setTime(st, t)
	return cp
}

// SetOperatorCurrentStatus copies the operator and sets the status of copy to the given status.
// NOTE: Shoud only use in test.
func SetOperatorCurrentStatus(op *Operator, st OpStatus) *Operator {
	cp := CopyOperator(op)
	cp.status.current = st
	return cp
}
