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
	"github.com/coreos/go-semver/semver"
	log "github.com/sirupsen/logrus"
)

// Feature supported features.
type Feature int

// Fetures list.
const (
	Base Feature = iota
	Version2_0
	RegionMerge
	RaftLearner
	BatchSplit
)

var featuresDict = map[Feature]semver.Version{
	Base:        {Major: 1},
	Version2_0:  {Major: 2},
	RegionMerge: {Major: 2, Minor: 0},
	RaftLearner: {Major: 2, Minor: 0},
	BatchSplit:  {Major: 2, Minor: 1},
}

// MinSupportedVersion returns the minimum support version for the specified feature.
func MinSupportedVersion(v Feature) semver.Version {
	target, ok := featuresDict[v]
	if !ok {
		log.Fatalf("version not exist, feature %d", v)
	}
	return target
}

// ParseVersion warpping semver.NewVersion and handle compatibility issues.
func ParseVersion(v string) (*semver.Version, error) {
	// for compatibility with old version which not support `version` mechanism.
	baseVersion := MinSupportedVersion(Base)
	if v == "" {
		return &baseVersion, nil
	}
	return semver.NewVersion(v)
}

// MustParseVersion wrapping ParseVersion and will panic if err is not nil.
func MustParseVersion(v string) *semver.Version {
	ver, err := ParseVersion(v)
	if err != nil {
		log.Fatalf("version string is illegal: %s", err)
	}
	return ver
}

// IsCompatible checks if the clusterVersion is compatible with the specify version.
func IsCompatible(clusterVersion, v semver.Version) bool {
	if clusterVersion.LessThan(v) {
		return true
	}
	return clusterVersion.Major == v.Major && clusterVersion.Minor == v.Minor
}
