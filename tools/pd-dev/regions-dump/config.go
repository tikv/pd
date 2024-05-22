// Copyright 2024 TiKV Project Authors.
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

package regiondump

import (
	flag "github.com/spf13/pflag"
	"github.com/tikv/pd/tools/pd-dev/util"
)

// Config is the heartbeat-bench configuration.
type Config struct {
	*util.GeneralConfig
	ClusterID uint64 `toml:"cluster-id" json:"cluster-id"`
	FilePath  string `toml:"file-path" json:"file-path"`
	StartID   uint64 `toml:"start-id" json:"start-id"`
	EndID     uint64 `toml:"end-id" json:"end-id"`
}

// NewConfig return a set of settings.
func newConfig() *Config {
	cfg := &Config{
		GeneralConfig: &util.GeneralConfig{},
	}
	cfg.FlagSet = flag.NewFlagSet("stores-dump", flag.ContinueOnError)
	fs := cfg.FlagSet
	cfg.GeneralConfig = util.NewGeneralConfig(fs)
	fs.ParseErrorsWhitelist.UnknownFlags = true
	fs.Uint64Var(&cfg.ClusterID, "cluster-id", 0, "please make cluster ID match with TiKV")
	fs.StringVar(&cfg.FilePath, "file", "stores.dump", "dump file path and name")
	fs.Uint64Var(&cfg.StartID, "start-id", 0, "ID of the start region")
	fs.Uint64Var(&cfg.EndID, "end-id", 0, "ID of the last region")

	return cfg
}
