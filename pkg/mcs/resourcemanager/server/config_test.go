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

package server

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	mconfig "github.com/pingcap/metering_sdk/config"

	"github.com/tikv/pd/pkg/utils/logutil"
)

func TestControllerConfig(t *testing.T) {
	re := require.New(t)
	cfgData := `
[controller]
ltb-max-wait-duration = "60s"
ltb-token-rpc-max-delay = "500ms"
degraded-mode-wait-duration = "2s"
[controller.request-unit]
read-base-cost = 1.0
read-cost-per-byte = 2.0
write-base-cost = 3.0
write-cost-per-byte = 4.0 
read-cpu-ms-cost =  5.0
`
	cfg := NewConfig()
	meta, err := toml.Decode(cfgData, &cfg)
	re.NoError(err)
	err = cfg.Adjust(&meta)
	re.NoError(err)

	re.Equal(2*time.Second, cfg.Controller.DegradedModeWaitDuration.Duration)
	re.Equal(60*time.Second, cfg.Controller.LTBMaxWaitDuration.Duration)
	re.Equal(500*time.Millisecond, cfg.Controller.LTBTokenRPCMaxDelay.Duration)
	re.LessOrEqual(math.Abs(cfg.Controller.RequestUnit.CPUMsCost-5), 1e-7)
	re.LessOrEqual(math.Abs(cfg.Controller.RequestUnit.WriteCostPerByte-4), 1e-7)
	re.LessOrEqual(math.Abs(cfg.Controller.RequestUnit.WriteBaseCost-3), 1e-7)
	re.LessOrEqual(math.Abs(cfg.Controller.RequestUnit.ReadCostPerByte-2), 1e-7)
	re.LessOrEqual(math.Abs(cfg.Controller.RequestUnit.ReadBaseCost-1), 1e-7)
}

func TestRedactInfoLogDefault(t *testing.T) {
	re := require.New(t)
	for _, tc := range []struct {
		data     string
		expected logutil.RedactInfoLogType
	}{
		{"", logutil.RedactInfoLogON},
		{"[security]\nredact-info-log = false", logutil.RedactInfoLogOFF},
	} {
		cfg := NewConfig()
		meta, err := toml.Decode(tc.data, cfg)
		re.NoError(err)
		re.NoError(cfg.Adjust(&meta))
		re.Equal(tc.expected, cfg.Security.RedactInfoLog, tc.data)
	}
}

func TestRedactedConfig(t *testing.T) {
	re := require.New(t)
	const (
		role          = "trn:iam::1:role/metering"
		tosAK         = "test-tos-ak"
		tosSK         = "test-tos-sk"
		tosToken      = "test-tos-token"
		redactedValue = "***REDACTED***"
	)

	re.Nil(RedactedConfig(nil))

	cfg := NewConfig()
	cfg.Metering = mconfig.MeteringConfig{
		Region: "cn-beijing",
		Bucket: "metering",
		TOS: &mconfig.MeteringTOSConfig{
			AssumeRoleARN:   role,
			AccessKey:       tosAK,
			SecretAccessKey: tosSK,
			SessionToken:    tosToken,
		},
	}

	var buf bytes.Buffer
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&buf),
		zapcore.InfoLevel,
	))
	logger.Info("resource manager config", zap.Reflect("config", RedactedConfig(cfg)))
	re.NoError(logger.Sync())

	logged := buf.String()
	re.NotContains(logged, tosAK)
	re.NotContains(logged, tosSK)
	re.NotContains(logged, tosToken)

	var entry struct {
		Msg    string `json:"msg"`
		Config Config `json:"config"`
	}
	re.NoError(json.Unmarshal(buf.Bytes(), &entry))
	re.Equal("resource manager config", entry.Msg)
	re.NotNil(entry.Config.Metering.TOS)
	re.Equal(role, entry.Config.Metering.TOS.AssumeRoleARN)
	re.Equal(redactedValue, entry.Config.Metering.TOS.AccessKey)
	re.Equal(redactedValue, entry.Config.Metering.TOS.SecretAccessKey)
	re.Equal(redactedValue, entry.Config.Metering.TOS.SessionToken)

	re.Equal(tosAK, cfg.Metering.TOS.AccessKey)
	re.Equal(tosSK, cfg.Metering.TOS.SecretAccessKey)
	re.Equal(tosToken, cfg.Metering.TOS.SessionToken)
}
