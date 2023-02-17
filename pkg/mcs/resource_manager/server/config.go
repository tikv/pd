// Copyright 2023 TiKV Project Authors.
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

package server

import (
	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"go.uber.org/zap"
)

// Config is the configuration for the TSO.
type Config struct {
	BackendEndpoints string `toml:"backend-endpoints" json:"backend-endpoints"`
	ListenAddr       string `toml:"listen-addr" json:"listen-addr"`

	Metric metricutil.MetricConfig `toml:"metric" json:"metric"`

	// Log related config.
	Log log.Config `toml:"log" json:"log"`

	Logger   *zap.Logger
	LogProps *log.ZapProperties

	Security SecurityConfig `toml:"security" json:"security"`
}

// NewConfig creates a new config.
func NewConfig() *Config {
	return &Config{}
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(flagSet *pflag.FlagSet) error {
	// Load config file if specified.
	if configFile, _ := flagSet.GetString("config"); configFile != "" {
		_, err := c.configFromFile(configFile)
		if err != nil {
			return err
		}
	}

	// ignore the error check here
	adjustCommandlineString(flagSet, &c.Log.Level, "log-level")
	adjustCommandlineString(flagSet, &c.Log.File.Filename, "log-file")
	adjustCommandlineString(flagSet, &c.Metric.PushAddress, "metrics-addr")
	adjustCommandlineString(flagSet, &c.Security.CAPath, "cacert")
	adjustCommandlineString(flagSet, &c.Security.CertPath, "cert")
	adjustCommandlineString(flagSet, &c.Security.KeyPath, "key")
	adjustCommandlineString(flagSet, &c.BackendEndpoints, "backend-endpoints")
	adjustCommandlineString(flagSet, &c.ListenAddr, "listen-addr")

	// TODO: Implement the main function body
	return nil
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) (*toml.MetaData, error) {
	meta, err := toml.DecodeFile(path, c)
	return &meta, errors.WithStack(err)
}

// SecurityConfig indicates the security configuration for pd server
type SecurityConfig struct {
	grpcutil.TLSConfig
	// RedactInfoLog indicates that whether enabling redact log
	RedactInfoLog bool              `toml:"redact-info-log" json:"redact-info-log"`
	Encryption    encryption.Config `toml:"encryption" json:"encryption"`
}

func adjustCommandlineString(flagSet *pflag.FlagSet, v *string, name string) {
	if value, _ := flagSet.GetString(name); value != "" {
		*v = value
	}
}
