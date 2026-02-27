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

package statistics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHotPeerStatGetLoadBounds(t *testing.T) {
	re := require.New(t)

	stat := &HotPeerStat{
		Loads: []float64{1, 2},
	}
	re.Equal(1.0, stat.GetLoad(0))
	re.Equal(2.0, stat.GetLoad(1))
	re.Equal(0.0, stat.GetLoad(2))
	re.Equal(0.0, stat.GetLoad(-1))

	stat = &HotPeerStat{
		Loads:        []float64{3, 4},
		rollingLoads: []*dimStat{nil},
	}
	re.Equal(3.0, stat.GetLoad(0))
	re.Equal(4.0, stat.GetLoad(1))
	re.Equal(0.0, stat.GetLoad(2))
}

func TestHotPeerStatIsHotSkipsNilRollingLoads(t *testing.T) {
	re := require.New(t)
	interval := time.Second

	hotStat := newDimStat(interval, rollingWindowsSize)
	hotStat.add(10, interval)

	stat := &HotPeerStat{
		rollingLoads: []*dimStat{nil, hotStat},
	}
	thresholds := []float64{5, 5}
	re.True(stat.isHot(thresholds))
}
