// Copyright 2025 TiKV Project Authors.
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

package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

type metaServiceGroupTestCase struct {
	id            string
	delta         int
	expectedCount int
}

func mustRunTestCase(re *require.Assertions, store Storage, testCase metaServiceGroupTestCase) {
	re.NoError(store.RunInTxn(context.TODO(), func(txn kv.Txn) error {
		statusMap, err := store.LoadMetaServiceGroupStatus(txn, map[string]string{testCase.id: ""})
		if err != nil {
			return err
		}
		status, exists := statusMap[testCase.id]
		re.True(exists)
		status.AssignmentCount += testCase.delta
		return store.SaveMetaServiceGroupStatus(txn, testCase.id, status)
	}))
	re.NoError(store.RunInTxn(context.TODO(), func(txn kv.Txn) error {
		statusMap, err := store.LoadMetaServiceGroupStatus(txn, map[string]string{testCase.id: ""})
		if err != nil {
			return err
		}
		status, exists := statusMap[testCase.id]
		re.True(exists)
		re.Equal(testCase.expectedCount, status.AssignmentCount)
		return nil
	}))
}

func checkAssignmentCount(re *require.Assertions, store Storage, expectedAssignment map[string]int) {
	var (
		statusMap map[string]*endpoint.MetaServiceGroupStatus
		err       error
	)
	groups := make(map[string]string)
	for id := range expectedAssignment {
		groups[id] = ""
	}
	re.NoError(store.RunInTxn(context.TODO(), func(txn kv.Txn) error {
		statusMap, err = store.LoadMetaServiceGroupStatus(txn, groups)
		if err != nil {
			return err
		}
		return nil
	}))
	for id, count := range expectedAssignment {
		re.Contains(statusMap, id)
		re.Equal(count, statusMap[id].AssignmentCount)
	}
}

func TestMetaServiceGroupStorage(t *testing.T) {
	re := require.New(t)
	store := newMemoryBackend()

	testCases := []metaServiceGroupTestCase{
		{
			id:            "group1",
			delta:         1,
			expectedCount: 1,
		},
		{
			id:            "group1",
			delta:         1,
			expectedCount: 2,
		},
		{
			id:            "group2",
			delta:         1,
			expectedCount: 1,
		},
		{
			id:            "group1",
			delta:         -1,
			expectedCount: 1,
		},
		{
			id:            "group3",
			delta:         100,
			expectedCount: 100,
		},
		{
			id:            "group3",
			delta:         -1,
			expectedCount: 99,
		},
		{
			id:            "group1",
			delta:         -1,
			expectedCount: 0,
		},
	}
	result := map[string]int{
		"group1": 0,
		"group2": 1,
		"group3": 99,
	}
	checkAssignmentCount(re, store, map[string]int{})
	for testCase := range testCases {
		mustRunTestCase(re, store, testCases[testCase])
	}
	checkAssignmentCount(re, store, result)
	// should treat extra groups as having 0 assignment.
	extraGroups := map[string]int{
		"group4": 0,
		"group5": 0,
	}
	checkAssignmentCount(re, store, extraGroups)

	testGroup1 := "group1"
	re.NoError(store.RunInTxn(context.TODO(), func(txn kv.Txn) error {
		statusMap, err := store.LoadMetaServiceGroupStatus(txn, map[string]string{testGroup1: ""})
		if err != nil {
			return err
		}
		status, exists := statusMap[testGroup1]
		re.True(exists)
		// Meta service group should be disabled by default
		re.False(status.Enabled)
		// Set enable
		status.Enabled = true
		return store.SaveMetaServiceGroupStatus(txn, testGroup1, status)
	}))

	re.NoError(store.RunInTxn(context.TODO(), func(txn kv.Txn) error {
		statusMap, err := store.LoadMetaServiceGroupStatus(txn, map[string]string{testGroup1: ""})
		if err != nil {
			return err
		}
		status, exists := statusMap[testGroup1]
		re.True(exists)
		// Should now be enabled
		re.True(status.Enabled)
		return nil
	}))
}
