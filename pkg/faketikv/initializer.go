// Copyright 2017 PingCAP, Inc.
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

package faketikv

// Initializer defines an Init interface
// we can implement different case to initialize cluster
type Initializer interface {
	Init(args ...string) *ClusterInfo
}

// TiltCase will initialize cluster with all regions distributed in 3 nodes
type TiltCase struct {
	NodeNumber   int `toml:"node-number" json:"node-number"`
	RegionNumber int `toml:"region-number" json:"region-number"`
}

// NewTiltCase returns tiltCase
func NewTiltCase() *TiltCase {}

// Init implement Initializer
func (c *TiltCase) Init(addr string, args ...string) *ClusterInfo {}
