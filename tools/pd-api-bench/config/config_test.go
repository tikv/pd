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
	"os"
	"path/filepath"
	"testing"

	flag "github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestParseKeyFormatFromConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "api-bench.toml")
	require.NoError(t, os.WriteFile(path, []byte(`key-format = "table"`), 0o600))

	flagSet := flag.NewFlagSet("api-bench-test", flag.ContinueOnError)
	cfg := NewConfig(flagSet)
	require.NoError(t, cfg.Parse([]string{"--config", path}))

	require.Equal(t, "table", cfg.KeyFormat)
}
