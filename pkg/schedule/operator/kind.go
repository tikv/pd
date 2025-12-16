// Copyright 2017 TiKV Project Authors.
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

package operator

import (
	"strings"

	"github.com/pingcap/errors"
)

// OpWaiting is the status of a waiting operators.
const OpWaiting = "waiting"

// OpKind is a bit field to identify operator types.
type OpKind uint32

// Flags for operators.
const (
	// Initiated by admin.
	OpAdmin OpKind = 1 << iota
	// Initiated by merge checker or merge scheduler. Note that it may not include region merge.
	// the order describe the operator's producer and is very helpful to decouple scheduler or checker limit
	OpMerge
	// Initiated by range scheduler.
	OpRange
	// Initiated by replica checker.
	OpReplica
	// Include region split. Initiated by rule checker if `kind & OpAdmin == 0`.
	OpSplit
	// Initiated by hot region scheduler.
	OpHotRegion
	// Include peer addition or removal or switch witness. This means that this operator may take a long time.
	OpRegion
	// Include leader transfer.
	OpLeader
	// Include witness leader transfer.
	OpWitnessLeader
	// Include witness transfer.
	OpWitness
	// Initiated by affinity checker.
	OpAffinity
	opMax
)

var flagToName = map[OpKind]string{
	OpLeader:        "leader",
	OpRegion:        "region",
	OpSplit:         "split",
	OpAdmin:         "admin",
	OpHotRegion:     "hot-region",
	OpReplica:       "replica",
	OpMerge:         "merge",
	OpRange:         "range",
	OpWitness:       "witness",
	OpWitnessLeader: "witness-leader",
	OpAffinity:      "affinity",
}

var nameToFlag = map[string]OpKind{
	"leader":         OpLeader,
	"region":         OpRegion,
	"split":          OpSplit,
	"admin":          OpAdmin,
	"hot-region":     OpHotRegion,
	"replica":        OpReplica,
	"merge":          OpMerge,
	"range":          OpRange,
	"witness-leader": OpWitnessLeader,
	"affinity":       OpAffinity,
}

func (k OpKind) String() string {
	var flagNames []string
	for flag := OpKind(1); flag < opMax; flag <<= 1 {
		if k&flag != 0 {
			flagNames = append(flagNames, flagToName[flag])
		}
	}
	if len(flagNames) == 0 {
		return "unknown"
	}
	return strings.Join(flagNames, ",")
}

// ParseOperatorKind converts string (flag name list concat by ',') to OpKind.
func ParseOperatorKind(str string) (OpKind, error) {
	var k OpKind
	for _, flagName := range strings.Split(str, ",") {
		flag, ok := nameToFlag[flagName]
		if !ok {
			return 0, errors.Errorf("unknown flag name: %s", flagName)
		}
		k |= flag
	}
	return k, nil
}
