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

package gc

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/storage/kv"
)

func newGCStorage() endpoint.GCSafePointStorage {
	return endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
}

func TestGCSafePointUpdateSequentially(t *testing.T) {
	gcSafePointManager := newGCSafePointManager(newGCStorage())
	re := require.New(t)
	curSafePoint := uint64(0)
	// update gc safePoint with asc value.
	for id := 10; id < 20; id++ {
		safePoint, err := gcSafePointManager.LoadGCSafePoint()
		re.NoError(err)
		re.Equal(curSafePoint, safePoint)
		previousSafePoint := curSafePoint
		curSafePoint = uint64(id)
		oldSafePoint, err := gcSafePointManager.UpdateGCSafePoint(curSafePoint)
		re.NoError(err)
		re.Equal(previousSafePoint, oldSafePoint)
	}

	safePoint, err := gcSafePointManager.LoadGCSafePoint()
	re.NoError(err)
	re.Equal(curSafePoint, safePoint)
	// update with smaller value should be failed.
	oldSafePoint, err := gcSafePointManager.UpdateGCSafePoint(safePoint - 5)
	re.NoError(err)
	re.Equal(safePoint, oldSafePoint)
	curSafePoint, err = gcSafePointManager.LoadGCSafePoint()
	re.NoError(err)
	// current safePoint should not change since the update value was smaller
	re.Equal(safePoint, curSafePoint)
}

func TestGCSafePointUpdateCurrently(t *testing.T) {
	gcSafePointManager := newGCSafePointManager(newGCStorage())
	maxSafePoint := uint64(1000)
	wg := sync.WaitGroup{}
	re := require.New(t)

	// update gc safePoint concurrently
	for id := 0; id < 20; id++ {
		wg.Add(1)
		go func(step uint64) {
			for safePoint := step; safePoint <= maxSafePoint; safePoint += step {
				_, err := gcSafePointManager.UpdateGCSafePoint(safePoint)
				re.NoError(err)
			}
			wg.Done()
		}(uint64(id + 1))
	}
	wg.Wait()
	safePoint, err := gcSafePointManager.LoadGCSafePoint()
	re.NoError(err)
	re.Equal(maxSafePoint, safePoint)
}
