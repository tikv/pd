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

package cases

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/codec"
)

func TestGRPCCaseMapIncludesBatchScanRegions(t *testing.T) {
	create, ok := GRPCCaseFnMap["BatchScanRegions"]
	require.True(t, ok)
	require.Equal(t, "BatchScanRegions", create().getName())
}

func TestSetKeyFormatTableMatchesHeartbeatBenchKeys(t *testing.T) {
	require.NoError(t, SetKeyFormat("table"))
	t.Cleanup(func() {
		require.NoError(t, SetKeyFormat("raw"))
	})

	require.Equal(t, codec.GenerateTableKey(42), generateKeyForSimulator(42))
}

func TestSetKeyFormatRejectsUnknownFormat(t *testing.T) {
	require.Error(t, SetKeyFormat("unknown"))
}

func TestGenerateRegionKeyIDUsesTableRegionIndexDirectly(t *testing.T) {
	require.NoError(t, SetKeyFormat("table"))
	t.Cleanup(func() {
		require.NoError(t, SetKeyFormat("raw"))
	})

	require.Equal(t, 42, generateRegionKeyID(42))
}

func TestGenerateRegionKeyIDKeepsRawSpacing(t *testing.T) {
	require.NoError(t, SetKeyFormat("raw"))

	require.Equal(t, 169, generateRegionKeyID(42))
}
