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

package gctuner

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCalcGCPercent(t *testing.T) {
	const gb = 1024 * 1024 * 1024
	// use default value when invalid params
	require.Equal(t, defaultGCPercent, calcGCPercent(0, 0))
	require.Equal(t, defaultGCPercent, calcGCPercent(0, 1))
	require.Equal(t, defaultGCPercent, calcGCPercent(1, 0))

	require.Equal(t, maxGCPercent.Load(), calcGCPercent(1, 3*gb))
	require.Equal(t, maxGCPercent.Load(), calcGCPercent(gb/10, 4*gb))
	require.Equal(t, maxGCPercent.Load(), calcGCPercent(gb/2, 4*gb))
	require.Equal(t, uint32(300), calcGCPercent(1*gb, 4*gb))
	require.Equal(t, uint32(166), calcGCPercent(1.5*gb, 4*gb))
	require.Equal(t, uint32(100), calcGCPercent(2*gb, 4*gb))
	require.Equal(t, uint32(100), calcGCPercent(3*gb, 4*gb))
	require.Equal(t, minGCPercent.Load(), calcGCPercent(4*gb, 4*gb))
	require.Equal(t, minGCPercent.Load(), calcGCPercent(5*gb, 4*gb))
}
