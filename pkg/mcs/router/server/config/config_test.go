// Copyright 2026 TiKV Project Authors.
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
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/utils/logutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
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
		re.NoError(cfg.adjust(&meta))
		re.Equal(tc.expected, cfg.Security.RedactInfoLog, tc.data)
	}
}
