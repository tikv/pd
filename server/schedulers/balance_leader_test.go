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

package schedulers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBalanceLeaderSchedulerConfigClone(t *testing.T) {
	re := require.New(t)
	keyRanges1, _ := getKeyRanges([]string{"a", "b", "c", "d"})
	conf := &balanceLeaderSchedulerConfig{
		Ranges: keyRanges1,
		Batch:  10,
	}
	con2 := conf.Clone()
	re.Equal(conf.Batch, con2.Batch)
	re.Equal(conf.Ranges, con2.Ranges)

	keyRanges2, _ := getKeyRanges([]string{"e", "f", "g", "h"})
	// update conf2
	con2.Ranges[1] = keyRanges2[1]
	re.NotEqual(conf.Ranges, con2.Ranges)
}
