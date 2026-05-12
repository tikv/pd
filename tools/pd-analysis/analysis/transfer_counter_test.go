// Copyright 2019 TiKV Project Authors.
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

package analysis

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func addData(test [][]uint64) {
	for i, row := range test {
		for j, flow := range row {
			for range flow {
				GetTransferCounter().AddTarget(64, uint64(j))
				GetTransferCounter().AddSource(64, uint64(i))
			}
		}
	}
}

func TestCounterRedundant(t *testing.T) {
	re := require.New(t)
	{
		test := [][]uint64{
			{0, 0, 0, 0, 0, 0, 0},
			{0, 0, 1, 1, 4, 0, 0},
			{0, 0, 0, 0, 8, 9, 6},
			{0, 0, 1, 0, 0, 3, 2},
			{0, 2, 3, 4, 0, 3, 0},
			{0, 5, 9, 0, 0, 0, 0},
			{0, 0, 8, 0, 0, 0, 0}}
		GetTransferCounter().Init(6, 3000)
		re.Equal(uint64(0), GetTransferCounter().Redundant)
		re.Equal(uint64(0), GetTransferCounter().Necessary)
		addData(test)
		GetTransferCounter().Result()
		re.Equal(uint64(64), GetTransferCounter().Redundant)
		re.Equal(uint64(5), GetTransferCounter().Necessary)
	}
	{
		test := [][]uint64{
			{0, 0, 0, 0, 0, 0, 0},
			{0, 0, 0, 0, 0, 0, 0},
			{0, 0, 0, 0, 0, 2, 0},
			{0, 0, 0, 0, 0, 0, 1},
			{0, 0, 0, 0, 0, 0, 0},
			{0, 1, 0, 0, 0, 0, 0},
			{0, 0, 1, 0, 0, 0, 0}}
		GetTransferCounter().Init(6, 3000)
		re.Equal(uint64(0), GetTransferCounter().Redundant)
		re.Equal(uint64(0), GetTransferCounter().Necessary)
		addData(test)
		GetTransferCounter().Result()
		re.Equal(uint64(0), GetTransferCounter().Redundant)
		re.Equal(uint64(5), GetTransferCounter().Necessary)
	}
	{
		test := [][]uint64{
			{0, 0, 0, 0, 0, 0, 0, 0, 0},
			{0, 0, 15, 42, 21, 84, 9, 38, 5},
			{0, 76, 0, 84, 3, 130, 0, 129, 0},
			{0, 0, 35, 0, 86, 0, 60, 0, 15},
			{0, 143, 0, 106, 0, 178, 0, 132, 0},
			{0, 0, 101, 0, 120, 0, 118, 1, 33},
			{0, 133, 0, 140, 0, 93, 0, 114, 0},
			{0, 0, 48, 0, 84, 1, 48, 0, 20},
			{0, 61, 2, 57, 7, 122, 1, 21, 0}}
		GetTransferCounter().Init(8, 3000)
		re.Equal(uint64(0), GetTransferCounter().Redundant)
		re.Equal(uint64(0), GetTransferCounter().Necessary)
		addData(test)
		GetTransferCounter().Result()
		re.Equal(uint64(1778), GetTransferCounter().Redundant)
		re.Equal(uint64(938), GetTransferCounter().Necessary)
		GetTransferCounter().PrintResult()
	}
}
