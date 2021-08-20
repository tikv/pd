// Copyright 2020 TiKV Project Authors.
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

package versioninfo

import (
	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
)

const (
	// CommunityEdition is the default edition for building.
	CommunityEdition = "Community"
)

// Version information.
var (
	PDReleaseVersion = "None"
	PDBuildTS        = "None"
	PDGitHash        = "None"
	PDGitBranch      = "None"
	PDEdition        = CommunityEdition
)

// ParseVersion wraps semver.NewVersion and handles compatibility issues.
func ParseVersion(v string) (*semver.Version, error) {
	// for compatibility with old version which not support `version` mechanism.
	if v == "" {
		return semver.New(featuresDict[Base]), nil
	}
	if v[0] == 'v' {
		v = v[1:]
	}
	ver, err := semver.NewVersion(v)
	if err != nil {
		return nil, errs.ErrSemverNewVersion.Wrap(err).GenWithStackByCause()
	}
	return ver, nil
}

// MustParseVersion wraps ParseVersion and will panic if error is not nil.
func MustParseVersion(v string) *semver.Version {
	ver, err := ParseVersion(v)
	if err != nil {
		log.Fatal("version string is illegal", errs.ZapError(err))
	}
	return ver
}

// IsCompatible checks if the version a is compatible with the version b.
func IsCompatible(a, b semver.Version) bool {
	if a.LessThan(b) {
		return true
	}
	return a.Major == b.Major && a.Minor == b.Minor
}
