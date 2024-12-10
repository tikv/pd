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
	"math"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
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
