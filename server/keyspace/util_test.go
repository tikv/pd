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

package keyspace

import (
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateID(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		id     uint32
		hasErr bool
	}{
		{DefaultKeyspaceID, true}, // Reserved id should result in error.
		{100, false},
		{spaceIDMax - 1, false},
		{spaceIDMax, false},
		{spaceIDMax + 1, true},
		{math.MaxUint32, true},
	}
	for _, testCase := range testCases {
		re.Equal(testCase.hasErr, validateID(testCase.id) != nil)
	}
}

func TestValidateName(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		name   string
		hasErr bool
	}{
		{DefaultKeyspaceName, true}, // Reserved name should result in error.
		{"keyspaceName1", false},
		{"keyspace_name_1", false},
		{"10", false},
		{"", true},
		{"keyspace/", true},
		{"keyspace:1", true},
		{"many many spaces", true},
		{"keyspace?limit=1", true},
		{"keyspace%1", true},
	}
	for _, testCase := range testCases {
		re.Equal(testCase.hasErr, validateName(testCase.name) != nil)
	}
}

func TestLockGroup(t *testing.T) {
	re := require.New(t)
	group := newLockGroup()
	concurrency := 20
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(spaceID uint32) {
			defer wg.Done()
			mustSequentialUpdateSingle(re, spaceID, group)
		}(rand.Uint32())
	}
	wg.Wait()
	// Check that size of the lock group is limited.
	re.LessOrEqual(len(group.entries), 1<<8)
}

// mustSequentialUpdateSingle checks that for any given update, update is sequential.
func mustSequentialUpdateSingle(re *require.Assertions, spaceID uint32, group *lockGroup) {
	concurrency := 100
	total := 0
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			group.lock(spaceID)
			defer group.unlock(spaceID)
			total++
		}()
	}
	wg.Wait()
	re.Equal(concurrency, total)
}
