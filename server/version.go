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
)

// VersionBase is the empty version
var VersionBase = Version{Marjor: 1}

// Version record version information.
type Version struct {
	Marjor        int32
	Minor         int32
	Patch         int32
	Unstable      int32
	UnstablePatch int32
}

// Less compare two version.
func (v *Version) Less(ov *Version) bool {
	if v.Marjor < ov.Marjor {
		return true
	} else if v.Minor < ov.Minor {
		return true
	} else if v.Patch < ov.Patch {
		return true
	} else if v.Unstable < ov.Unstable {
		return true
	} else if v.UnstablePatch < ov.UnstablePatch {
		return true
	}
	return false
}

func (v *Version) String() string {
	convMap := map[int32]string{
		1: "alpha",
		2: "beta",
		3: "rc",
	}
	var res string
	if unstable, ok := convMap[v.Unstable]; ok {
		res = fmt.Sprintf("%d.%d.%d-%s", v.Marjor, v.Minor, v.Patch, unstable)
		if v.UnstablePatch > 0 {
			res = fmt.Sprintf("%s.%d", res, v.UnstablePatch)
		}
		return res
	}
	res = fmt.Sprintf("%d.%d.%d", v.Marjor, v.Minor, v.Patch)
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
		if i == 4 && part != "0" {
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
		Marjor:        ints[0],
		Minor:         ints[1],
		Patch:         ints[2],
		Unstable:      ints[3],
		UnstablePatch: ints[4],
	}, nil
}

// MarshalJSON returns the version as a JSON string.
func (v *Version) MarshalJSON() ([]byte, error) {
	return []byte(v.String()), nil
}

// UnmarshalJSON parses a JSON string into the version.
func (v *Version) UnmarshalJSON(text []byte) error {
	s, err := strconv.Unquote(string(text))
	if err != nil {
		return errors.Trace(err)
	}
	version, err := ParseVersion(s)
	if err != nil {
		return errors.Trace(err)
	}
	*v = version
	return nil
}
