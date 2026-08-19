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
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
)

// Feature supported features.
type Feature int

// Features list.
// The cluster provides corresponding new features if the cluster version
// greater than or equal to the required minimum version of the feature.
const (
	Base Feature = iota
	Version2_0
	// RegionMerge supports the adjacent regions to be merged.
	// and PD will periodically check if there is enough small
	// region to be merged. if there is, will send the corresponding
	// merge command to the TiKV.
	RegionMerge
	// BatchSplit can speed up the region split.
	// and PD will response the BatchSplit request.
	BatchSplit
	Version3_0
	Version4_0
	Version5_0
	// JointConsensus is supported in ConfChangeV2, which supports safe conf change across data center.
	ConfChangeV2
	// HotScheduleWithQuery supports schedule hot region with query info.
	HotScheduleWithQuery
	// SwitchWithess supports switch between witness and non-witness.
	SwitchWitness
)

var featuresDict = map[Feature]string{
	Base:                 "1.0.0",
	Version2_0:           "2.0.0",
	RegionMerge:          "2.0.0",
	BatchSplit:           "2.1.0-rc.1",
	Version3_0:           "3.0.0",
	Version4_0:           "4.0.0",
	Version5_0:           "5.0.0",
	ConfChangeV2:         "5.0.0",
	HotScheduleWithQuery: "5.2.0",
	SwitchWitness:        "6.6.0",
}

// MinSupportedVersion returns the minimum support version for the specified feature.
func MinSupportedVersion(v Feature) *semver.Version {
	target, ok := featuresDict[v]
	if !ok {
		log.Fatal("the corresponding version of the feature doesn't exist", zap.Int("feature-number", int(v)), errs.ZapError(errs.ErrFeatureNotExisted))
	}
	version := MustParseVersion(target)
	return version
}
