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
	"fmt"
	"strings"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
)

// Status is the status of PD server.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type Status struct {
	BuildTS        string `json:"build_ts"`
	Version        string `json:"version"`
	GitHash        string `json:"git_hash"`
	StartTimestamp int64  `json:"start_timestamp"`
}

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

// IsFeatureSupported checks if the feature is supported by current cluster.
func IsFeatureSupported(clusterVersion *semver.Version, f Feature) bool {
	minSupportVersion := *MinSupportedVersion(f)
	// For features before version 5.0 (such as BatchSplit), strict version checks are performed according to the
	// original logic. But according to Semantic Versioning, specify a version MAJOR.MINOR.PATCH, PATCH is used when you
	// make backwards compatible bug fixes. In version 5.0 and later, we need to strictly comply.
	if IsCompatible(minSupportVersion, *MinSupportedVersion(Version4_0)) {
		return !clusterVersion.LessThan(minSupportVersion)
	}
	return IsCompatible(minSupportVersion, *clusterVersion)
}

// Log prints the version information of the PD with the specific service mode.
func Log(serviceMode string) {
	mode := strings.ToUpper(serviceMode)
	log.Info(fmt.Sprintf("Welcome to Placement Driver (%s)", mode))
	log.Info(mode, zap.String("release-version", PDReleaseVersion))
	log.Info(mode, zap.String("edition", PDEdition))
	log.Info(mode, zap.String("git-hash", PDGitHash))
	log.Info(mode, zap.String("git-branch", PDGitBranch))
	log.Info(mode, zap.String("utc-build-time", PDBuildTS))
}

// Print prints the version information, without log info, of the PD with the specific service mode.
func Print() {
	fmt.Println("Release Version:", PDReleaseVersion)
	fmt.Println("Edition:", PDEdition)
	fmt.Println("Git Commit Hash:", PDGitHash)
	fmt.Println("Git Branch:", PDGitBranch)
	fmt.Println("UTC Build Time: ", PDBuildTS)
}
