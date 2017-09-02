// Copyright 2016 PingCAP, Inc.
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

package schedule

import (
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
)

// Operator is an interface to schedule region.
type Operator interface {
	GetRegionID() uint64
	GetResourceKind() core.ResourceKind
	GetState() OperatorState
	SetState(OperatorState)
	GetName() string
	Do(region *core.RegionInfo) (*pdpb.RegionHeartbeatResponse, bool)
}

// MaxOperatorWaitTime is the duration that if an operator lives longer that it,
// the operator is considered timeout.
const MaxOperatorWaitTime = 5 * time.Minute

// OperatorState indicates state of the operator
type OperatorState int

const (
	// OperatorUnKnownState indicates the unknown state
	OperatorUnKnownState OperatorState = iota
	// OperatorWaiting indicates the waiting state
	OperatorWaiting
	// OperatorRunning indicates the doing state
	OperatorRunning
	// OperatorFinished indicates the finished state
	OperatorFinished
	// OperatorTimeOut indicates the time_out state
	OperatorTimeOut
	// OperatorReplaced indicates this operator replaced by more priority operator
	OperatorReplaced
)

var operatorStateToName = map[OperatorState]string{
	0: "unknown",
	1: "waiting",
	2: "running",
	3: "finished",
	4: "timeout",
	5: "replaced",
}

var operatorStateNameToValue = map[string]OperatorState{
	"unknown":  OperatorUnKnownState,
	"waiting":  OperatorWaiting,
	"running":  OperatorRunning,
	"finished": OperatorFinished,
	"timeout":  OperatorTimeOut,
	"replaced": OperatorReplaced,
}

func (o OperatorState) String() string {
	s, ok := operatorStateToName[o]
	if ok {
		return s
	}
	return operatorStateToName[OperatorUnKnownState]
}

// MarshalJSON returns the state as a JSON string
func (o OperatorState) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(o.String())), nil
}

// UnmarshalJSON  parses a JSON string into the OperatorState
func (o *OperatorState) UnmarshalJSON(text []byte) error {
	s, err := strconv.Unquote(string(text))
	if err != nil {
		return errors.Trace(err)
	}
	state, ok := operatorStateNameToValue[s]
	if !ok {
		*o = OperatorUnKnownState
	} else {
		*o = state
	}
	return nil
}
