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

package tso

import (
	"flag"
	"time"

	"github.com/tikv/pd/pkg/utils/typeutil"
)

const (
	// defaultTSOUpdatePhysicalInterval is the default value of the config `TSOUpdatePhysicalInterval`.
	defaultTSOUpdatePhysicalInterval = 50 * time.Millisecond
)

// Config is the configuration for the TSO.
type Config struct {
	flagSet *flag.FlagSet

	configFile string
	// EnableLocalTSO is used to enable the Local TSO Allocator feature,
	// which allows the PD server to generate Local TSO for certain DC-level transactions.
	// To make this feature meaningful, user has to set the "zone" label for the PD server
	// to indicate which DC this PD belongs to.
	EnableLocalTSO bool `toml:"enable-local-tso" json:"enable-local-tso"`

	// TSOSaveInterval is the interval to save timestamp.
	TSOSaveInterval typeutil.Duration `toml:"tso-save-interval" json:"tso-save-interval"`

	// The interval to update physical part of timestamp. Usually, this config should not be set.
	// At most 1<<18 (262144) TSOs can be generated in the interval. The smaller the value, the
	// more TSOs provided, and at the same time consuming more CPU time.
	// This config is only valid in 1ms to 10s. If it's configured too long or too short, it will
	// be automatically clamped to the range.
	TSOUpdatePhysicalInterval typeutil.Duration `toml:"tso-update-physical-interval" json:"tso-update-physical-interval"`
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("pd", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.StringVar(&cfg.configFile, "config", "", "config file")

	return cfg
}
