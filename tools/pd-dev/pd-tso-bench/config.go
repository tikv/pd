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

package tsobench

import (
	"time"

	flag "github.com/spf13/pflag"
	"github.com/tikv/pd/tools/pd-dev/util"
)

// Config is the heartbeat-bench configuration.
type config struct {
	*util.GeneralConfig
	Client      int           `toml:"client" json:"client"`
	Duration    time.Duration `toml:"duration" json:"duration"`
	Count       int           `toml:"count" json:"count"`
	Concurrency int           `toml:"concurrency" json:"concurrency"`
	DcLocation  string        `toml:"dc-location" json:"dc-location"`
	Verbose     bool          `toml:"verbose" json:"verbose"`
	Interval    time.Duration `toml:"interval" json:"interval"`

	MaxBatchWaitInterval           time.Duration `toml:"max-batch-wait-interval" json:"max-batch-wait-interval"`
	EnableTSOFollowerProxy         bool          `toml:"enable-tso-follower-proxy" json:"enable-tso-follower-proxy"`
	EnableFaultInjection           bool          `toml:"enable-fault-injection" json:"enable-fault-injection"`
	FaultInjectionRate             float64       `toml:"fault-injection-rate" json:"fault-injection-rate"`
	MaxTSOSendIntervalMilliseconds int           `toml:"max-tso-send-interval-ms" json:"max-tso-send-interval-ms"`
	KeyspaceID                     uint          `toml:"keyspace-id" json:"keyspace-id"`
	KeyspaceName                   string        `toml:"keyspace-name" json:"keyspace-name"`
}

// NewConfig return a set of settings.
func newConfig() *config {
	cfg := &config{
		GeneralConfig: &util.GeneralConfig{},
	}
	cfg.FlagSet = flag.NewFlagSet("api-bench", flag.ContinueOnError)
	fs := cfg.FlagSet
	cfg.GeneralConfig = util.NewGeneralConfig(fs)
	fs.ParseErrorsWhitelist.UnknownFlags = true

	fs.Int("client", 1, "the number of pd clients involved in each benchmark")
	fs.Int("concurrency", 1000, "concurrency")
	fs.Int("count", 1, "the count number that the test will run")
	fs.Duration("duration", 60*time.Second, "how many seconds the test will last")
	fs.String("dc", "global", "which dc-location this bench will request")
	fs.Bool("v", false, "output statistics info every interval and output metrics info at the end")
	fs.Duration("interval", time.Second, "interval to output the statistics")
	fs.Duration("batch-interval", 0, "the max batch wait interval")
	fs.Bool("enable-tso-follower-proxy", false, "whether enable the TSO Follower Proxy")
	fs.Bool("enable-fault-injection", false, "whether enable fault injection")
	fs.Float64("fault-injection-rate", 0.01, "the failure rate [0.0001, 1]. 0.01 means 1% failure rate")
	fs.Int("max-send-interval-ms", 0, "max tso send interval in milliseconds, 60s by default")
	fs.Uint("keyspace-id", 0, "the id of the keyspace to access")
	fs.String("keyspace-name", "", "the name of the keyspace to access")

	return cfg
}
