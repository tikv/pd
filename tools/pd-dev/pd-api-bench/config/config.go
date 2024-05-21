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

package config

import (
	"github.com/pingcap/log"
	flag "github.com/spf13/pflag"
	"github.com/tikv/pd/tools/pd-dev/pd-api-bench/cases"
	"github.com/tikv/pd/tools/pd-dev/util"
	"go.uber.org/zap"
)

// Config is the heartbeat-bench configuration.
type Config struct {
	*util.GeneralConfig
	Client int64 `toml:"client" json:"client"`

	QPS       int64
	Burst     int64
	HTTPCases string
	GRPCCases string

	// only for init
	HTTP map[string]cases.Config `toml:"http" json:"http"`
	GRPC map[string]cases.Config `toml:"grpc" json:"grpc"`
	ETCD map[string]cases.Config `toml:"etcd" json:"etcd"`
}

// NewConfig return a set of settings.
func NewConfig() *Config {
	cfg := &Config{
		GeneralConfig: &util.GeneralConfig{},
	}
	cfg.FlagSet = flag.NewFlagSet("api-bench", flag.ContinueOnError)
	fs := cfg.FlagSet
	cfg.GeneralConfig = util.NewGeneralConfig(fs)
	fs.ParseErrorsWhitelist.UnknownFlags = true
	fs.Int64Var(&cfg.QPS, "qps", 1, "qps")
	fs.Int64Var(&cfg.Burst, "burst", 1, "burst")
	fs.StringVar(&cfg.HTTPCases, "http-cases", "", "http api cases")
	fs.StringVar(&cfg.GRPCCases, "grpc-cases", "", "grpc cases")
	fs.Int64Var(&cfg.Client, "client", 1, "client number")
	return cfg
}

// InitCoordinator set case config from config itself.
func (c *Config) InitCoordinator(co *cases.Coordinator) {
	for name, cfg := range c.HTTP {
		err := co.SetHTTPCase(name, &cfg)
		if err != nil {
			log.Error("create HTTP case failed", zap.Error(err))
		}
	}
	for name, cfg := range c.GRPC {
		err := co.SetGRPCCase(name, &cfg)
		if err != nil {
			log.Error("create gRPC case failed", zap.Error(err))
		}
	}
	for name, cfg := range c.ETCD {
		err := co.SetETCDCase(name, &cfg)
		if err != nil {
			log.Error("create etcd case failed", zap.Error(err))
		}
	}
}
