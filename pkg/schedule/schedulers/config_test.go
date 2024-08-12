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

package schedulers

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/storage"
)

func TestSchedulerConfig(t *testing.T) {
	s := storage.NewStorageWithMemoryBackend()
	cfg := &baseSchedulerConfig{}
	cfg.init("test", s)

	type testConfig struct {
		Value string `json:"value"`
	}

	tc := &testConfig{
		Value: "test",
	}
	require.NoError(t, cfg.save(tc))
	newTc := &testConfig{}
	require.NoError(t, cfg.load(newTc))
	require.Equal(t, tc.Value, newTc.Value)
}
