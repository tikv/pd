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

package server

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestRUTracker(t *testing.T) {
	const floatDelta = 0.1
	re := require.New(t)

	rt := newRUTracker(time.Second)
	now := time.Now()
	rt.sample(now, 100)
	re.Zero(rt.getRUPerSec())
	now = now.Add(time.Second)
	rt.sample(now, 100)
	re.Equal(100.0, rt.getRUPerSec())
	now = now.Add(time.Second)
	rt.sample(now, 100)
	re.InDelta(100.0, rt.getRUPerSec(), floatDelta)
	now = now.Add(time.Second)
	rt.sample(now, 200)
	re.InDelta(150.0, rt.getRUPerSec(), floatDelta)
	// EMA should eventually converge to 10000 RU/s.
	const targetRUPerSec = 10000.0
	testutil.Eventually(re, func() bool {
		now = now.Add(time.Second)
		rt.sample(now, targetRUPerSec)
		return math.Abs(rt.getRUPerSec()-targetRUPerSec) < floatDelta
	})
}

func TestGroupRUTracker(t *testing.T) {
	const floatDelta = 0.1
	re := require.New(t)

	grt := newGroupRUTracker()
	re.NotNil(grt)
	re.NotNil(grt.ruTrackers)
	re.Empty(grt.ruTrackers)

	now := time.Now()
	for i := range 10 {
		clientUniqueID := uint64(i)
		grt.sample(clientUniqueID, now, 100)
		grt.sample(clientUniqueID, now.Add(time.Second), 100)
		re.InDelta(100.0, grt.getOrCreateRUTracker(clientUniqueID).getRUPerSec(), floatDelta)
	}
	re.Len(grt.ruTrackers, 10)

	manager := &Manager{groupRUTrackers: make(map[string]*groupRUTracker)}
	re.Same(manager.getOrCreateGroupRUTracker("rg1"), manager.getOrCreateGroupRUTracker("rg1"))
	re.NotSame(manager.getOrCreateGroupRUTracker("rg1"), manager.getOrCreateGroupRUTracker("rg2"))
}
