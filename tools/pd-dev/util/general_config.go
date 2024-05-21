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

package util

import (
	"crypto/tls"
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	flag "github.com/spf13/pflag"
	"github.com/tikv/pd/client/tlsutil"
	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"go.uber.org/zap"
)

var defaultLogFormat = "text"

type GeneralConfig struct {
	FlagSet    *flag.FlagSet
	ConfigFile string
	PDAddrs    string `toml:"pd-endpoints" json:"pd"`
	StatusAddr string `toml:"status" json:"status"`

	Log      log.Config `toml:"log" json:"log"`
	Logger   *zap.Logger
	LogProps *log.ZapProperties

	// tls
	CaPath   string `toml:"ca-path" json:"ca-path"`
	CertPath string `toml:"cert-path" json:"cert-path"`
	KeyPath  string `toml:"key-path" json:"key-path"`
}

// NewGeneralConfig return a set of settings.
func NewGeneralConfig(flagSet *flag.FlagSet) *GeneralConfig {
	cfg := &GeneralConfig{}
	cfg.FlagSet = flagSet
	fs := cfg.FlagSet
	fs.StringVar(&cfg.ConfigFile, "config", "", "config file")
	fs.StringVar(&cfg.PDAddrs, "pd-endpoints", "http://127.0.0.1:2379", "pd address")
	fs.StringVar(&cfg.Log.File.Filename, "log-file", "", "log file path")
	fs.StringVar(&cfg.StatusAddr, "status", "127.0.0.1:10081", "status address")
	fs.StringVar(&cfg.CaPath, "cacert", "", "path of file that contains list of trusted SSL CAs")
	fs.StringVar(&cfg.CertPath, "cert", "", "path of file that contains X509 certificate in PEM format")
	fs.StringVar(&cfg.KeyPath, "key", "", "path of file that contains X509 key in PEM format")
	return cfg
}

// Parse parses flag definitions from the argument list.
func (c *GeneralConfig) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		_, err = configutil.ConfigFromFile(c, c.ConfigFile)
		if err != nil {
			return err
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	c.Adjust()
	return nil
}

// Adjust is used to adjust configurations
func (c *GeneralConfig) Adjust() {
	if len(c.Log.Format) == 0 {
		c.Log.Format = defaultLogFormat
	}

	err := logutil.SetupLogger(c.Log, &c.Logger, &c.LogProps)
	if err == nil {
		log.ReplaceGlobals(c.Logger, c.LogProps)
	} else {
		log.Fatal("initialize logger error", zap.Error(err))
	}
}

func LoadTLSConfig(cfg *GeneralConfig) *tls.Config {
	if len(cfg.CaPath) == 0 {
		return nil
	}
	caData, err := os.ReadFile(cfg.CaPath)
	if err != nil {
		log.Error("fail to read ca file", zap.Error(err))
	}
	certData, err := os.ReadFile(cfg.CertPath)
	if err != nil {
		log.Error("fail to read cert file", zap.Error(err))
	}
	keyData, err := os.ReadFile(cfg.KeyPath)
	if err != nil {
		log.Error("fail to read key file", zap.Error(err))
	}

	tlsConf, err := tlsutil.TLSConfig{
		SSLCABytes:   caData,
		SSLCertBytes: certData,
		SSLKEYBytes:  keyData,
	}.ToTLSConfig()
	if err != nil {
		log.Fatal("failed to load tlc config", zap.Error(err))
	}

	return tlsConf
}
