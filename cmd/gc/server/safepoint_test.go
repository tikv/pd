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
	"sync"
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/storage/kv"
)

func TestStorage(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testGCManagerSuite{})

type testGCManagerSuite struct {
}

func newGCStorage() endpoint.GCSafePointStorage {
	return endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
}

func (s *testGCManagerSuite) TestGCSafePointUpdateSequentially(c *C) {
	gcSafePointManager := newGCSafePointManager(newGCStorage())
	curSafePoint := uint64(0)
	// update gc safePoint with asc value.
	for id := 10; id < 20; id++ {
		safePoint, err := gcSafePointManager.LoadGCSafePoint()
		c.Assert(err, IsNil)
		c.Assert(safePoint, Equals, curSafePoint)
		previousSafePoint := curSafePoint
		curSafePoint = uint64(id)
		oldSafePoint, err := gcSafePointManager.UpdateGCSafePoint(curSafePoint)
		c.Assert(err, IsNil)
		c.Assert(oldSafePoint, Equals, previousSafePoint)
	}

	safePoint, err := gcSafePointManager.LoadGCSafePoint()
	c.Assert(err, IsNil)
	c.Assert(safePoint, Equals, safePoint)
	// update with smaller value should be failed.
	oldSafePoint, err := gcSafePointManager.UpdateGCSafePoint(safePoint - 5)
	c.Assert(err, IsNil)
	c.Assert(oldSafePoint, Equals, safePoint)
	curSafePoint, err = gcSafePointManager.LoadGCSafePoint()
	c.Assert(err, IsNil)
	// current safePoint should not change since the update value was smaller
	c.Assert(curSafePoint, Equals, safePoint)
}

func (s *testGCManagerSuite) TestGCSafePointUpdateCurrently(c *C) {
	gcSafePointManager := newGCSafePointManager(newGCStorage())
	maxSafePoint := uint64(1000)
	wg := sync.WaitGroup{}

	// update gc safePoint concurrently
	for id := 0; id < 20; id++ {
		wg.Add(1)
		go func(step uint64) {
			for safePoint := step; safePoint <= maxSafePoint; safePoint += step {
				_, err := gcSafePointManager.UpdateGCSafePoint(safePoint)
				c.Assert(err, IsNil)
			}
			wg.Done()
		}(uint64(id + 1))
	}
	wg.Wait()
	safePoint, err := gcSafePointManager.LoadGCSafePoint()
	c.Assert(err, IsNil)
	c.Assert(safePoint, Equals, maxSafePoint)
}
