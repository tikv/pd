// Copyright 2018 PingCAP, Inc.
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

package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

// VersionFeature is a unique identifier for minimum support version.
type VersionFeature int

// Version Fetures.
const (
	VersionBase VersionFeature = iota
	VersionRegionMergeAndRaftLearner
	VersionBatchSplit
)

var featuresDict = map[VersionFeature]Version{
	VersionBase:                      {Major: 1},
	VersionRegionMergeAndRaftLearner: {Major: 2, Minor: 0},
	VersionBatchSplit:                {Major: 2, Minor: 1},
}

// TargetVersion get target version by version feature
func TargetVersion(v VersionFeature) Version {
	target, ok := featuresDict[v]
	if !ok {
		log.Fatalf("version not exist, feature %d", v)
	}
	return target
}

// Version record version information.
type Version struct {
	Major         int32
	Minor         int32
	Patch         int32
	Unstable      int32
	UnstablePatch int32
}

// Less compare two version.
func (v *Version) Less(ov Version) bool {
	if v.Major < ov.Major {
		return true
	} else if v.Major > ov.Major {
		return false
	}
	if v.Minor < ov.Minor {
		return true
	} else if v.Minor > ov.Minor {
		return false
	}
	if v.Patch < ov.Patch {
		return true
	} else if v.Patch > ov.Patch {
		return false
	}
	if v.Unstable < ov.Unstable {
		return true
	} else if v.Unstable > ov.Unstable {
		return false
	}
	if v.UnstablePatch < ov.UnstablePatch {
		return true
	}
	return false
}

func (v Version) String() string {
	convMap := map[int32]string{
		1: "alpha",
		2: "beta",
		3: "rc",
	}
	var res string
	if unstable, ok := convMap[v.Unstable]; ok {
		res = fmt.Sprintf("v%d.%d.%d-%s", v.Major, v.Minor, v.Patch, unstable)
		if v.UnstablePatch > 0 {
			res = fmt.Sprintf("%s.%d", res, v.UnstablePatch)
		}
		return res
	}
	res = fmt.Sprintf("v%d.%d.%d", v.Major, v.Minor, v.Patch)
	return res
}

func convUnstable(s string) int32 {
	innerMap := map[string]int32{
		"alpha": 1,
		"beta":  2,
		"rc":    3,
	}
	if res, ok := innerMap[s]; ok {
		return res
	}
	return -1
}

//ParseVersion parses a version from a string.
//The string format should be "major.minor.patch-<unstable>".
func ParseVersion(s string) (Version, error) {
	if s == "" {
		return TargetVersion(VersionBase), nil
	}
	if strings.HasPrefix(s, "v") {
		s = s[1:]
	}
	splits := strings.Split(s, ".")
	if len(splits) != 3 && len(splits) != 4 {
		return Version{}, errors.Errorf("invalid version: %s", s)
	}
	patchAndUnstable := strings.Split(splits[2], "-")

	parts := append(splits[:2], patchAndUnstable...)
	if len(splits) == 4 {
		parts = append(parts, splits[3])
	}
	for i := len(parts); i < 5; i++ {
		parts = append(parts, "0")
	}
	ints := make([]int32, len(parts))
	for i, part := range parts {
		if i == 3 && part != "0" {
			r := convUnstable(part)
			if r < 0 {
				return Version{}, errors.Errorf("invalid version: %s ", s)
			}
			ints[i] = r
			continue
		}
		var (
			val int64
			err error
		)
		if val, err = strconv.ParseInt(part, 10, 32); err != nil {
			return Version{}, errors.Errorf("invalid version: %s ", s)
		}
		ints[i] = int32(val)
	}
	return Version{
		Major:         ints[0],
		Minor:         ints[1],
		Patch:         ints[2],
		Unstable:      ints[3],
		UnstablePatch: ints[4],
	}, nil
}
