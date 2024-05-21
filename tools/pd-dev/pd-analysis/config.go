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

package analysis

import (
	flag "github.com/spf13/pflag"
	"github.com/tikv/pd/tools/pd-dev/util"
)

// Config is the heartbeat-bench configuration.
type Config struct {
	*util.GeneralConfig
	Input    string `toml:"input" json:"input"`
	Output   string `toml:"output" json:"output"`
	Style    string `toml:"style" json:"style"`
	Operator string `toml:"operator" json:"operator"`
	Start    string `toml:"start" json:"start"`
	End      string `toml:"end" json:"end"`
}

// NewConfig return a set of settings.
func newConfig() *Config {
	cfg := &Config{
		GeneralConfig: &util.GeneralConfig{},
	}
	cfg.FlagSet = flag.NewFlagSet("pd-analysis", flag.ContinueOnError)
	fs := cfg.FlagSet
	cfg.GeneralConfig = util.NewGeneralConfig(fs)
	fs.ParseErrorsWhitelist.UnknownFlags = true

	fs.StringVar(&cfg.Input, "input", "", "input pd log file, required")
	fs.StringVar(&cfg.Output, "output", "", "output file, default output to stdout")
	fs.StringVar(&cfg.Style, "style", "", "analysis style, e.g. transfer-counter")
	fs.StringVar(&cfg.Operator, "operator", "", "operator style, e.g. balance-region, balance-leader, transfer-hot-read-leader, move-hot-read-region, transfer-hot-write-leader, move-hot-write-region")
	fs.StringVar(&cfg.Start, "start", "", "start time, e.g. 2019/09/10 12:20:07, default: total file")
	fs.StringVar(&cfg.End, "end", "", "end time, e.g. 2019/09/10 14:20:07, default: total file")

	return cfg
}
