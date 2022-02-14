// Copyright 2022 TiKV Project Authors.
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

package config

// ImmutableConfig is a readonly config.
type ImmutableConfig struct {
	maxRegionSize uint64
	leaderLease   int64
}

// NewImmutableConfig creates a new immutable config.
func NewImmutableConfig(cfg *Config, ops ...ImmutableConfigCreateOption) *ImmutableConfig {
	config := &ImmutableConfig{
		maxRegionSize: cfg.MaxRegionSize,
		leaderLease:   cfg.LeaderLease,
	}
	for _, op := range ops {
		op(config)
	}
	return config
}

// GetMaxRegionSize returns the max region size.
func (c *ImmutableConfig) GetMaxRegionSize() uint64 {
	return c.maxRegionSize
}

// ImmutableConfigCreateOption used to create ImmutableConfig.
type ImmutableConfigCreateOption func(config *ImmutableConfig)

// WithMaxRegionSize set max region size.
func WithMaxRegionSize(maxRegionSize uint64) ImmutableConfigCreateOption {
	return func(config *ImmutableConfig) {
		config.maxRegionSize = maxRegionSize
	}
}
