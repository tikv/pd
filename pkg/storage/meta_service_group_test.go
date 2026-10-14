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

package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

type metaServiceGroupTestCase struct {
	id       string
	status   *endpoint.MetaServiceGroupStatus
	expected *endpoint.MetaServiceGroupStatus
}

func mustRunTestCase(re *require.Assertions, store Storage, testCase metaServiceGroupTestCase) {
	re.NoError(store.RunInTxn(context.TODO(), func(txn kv.Txn) error {
		return store.SaveMetaServiceGroupStatus(txn, testCase.id, testCase.status)
	}))
	var currentStatus *endpoint.MetaServiceGroupStatus
	re.NoError(store.RunInTxn(context.TODO(), func(txn kv.Txn) error {
		status, err := store.LoadMetaServiceGroupStatus(txn, map[string]string{testCase.id: ""})
		if err != nil {
			return err
		}
		currentStatus = status[testCase.id]
		return nil
	}))
	re.Equal(testCase.expected, currentStatus)
}

func checkStatus(re *require.Assertions, store Storage, expectedStatus map[string]*endpoint.MetaServiceGroupStatus) {
	var (
		currentStatus map[string]*endpoint.MetaServiceGroupStatus
		err           error
	)
	groups := make(map[string]string)
	for id := range expectedStatus {
		groups[id] = ""
	}
	re.NoError(store.RunInTxn(context.TODO(), func(txn kv.Txn) error {
		currentStatus, err = store.LoadMetaServiceGroupStatus(txn, groups)
		if err != nil {
			return err
		}
		return nil
	}))
	re.Equal(expectedStatus, currentStatus)
}

func TestMetaServiceGroupStorage(t *testing.T) {
	re := require.New(t)
	store := newMemoryBackend()

	testCases := []metaServiceGroupTestCase{
		{
			id:       "group1",
			status:   &endpoint.MetaServiceGroupStatus{AssignmentCount: 1, Enabled: true},
			expected: &endpoint.MetaServiceGroupStatus{AssignmentCount: 1, Enabled: true},
		},
		{
			id:       "group1",
			status:   &endpoint.MetaServiceGroupStatus{AssignmentCount: 2, Enabled: true},
			expected: &endpoint.MetaServiceGroupStatus{AssignmentCount: 2, Enabled: true},
		},
		{
			id:       "group2",
			status:   &endpoint.MetaServiceGroupStatus{AssignmentCount: 1, Enabled: false},
			expected: &endpoint.MetaServiceGroupStatus{AssignmentCount: 1, Enabled: false},
		},
		{
			id:       "group1",
			status:   &endpoint.MetaServiceGroupStatus{AssignmentCount: 1, Enabled: false},
			expected: &endpoint.MetaServiceGroupStatus{AssignmentCount: 1, Enabled: false},
		},
		{
			id:       "group3",
			status:   &endpoint.MetaServiceGroupStatus{AssignmentCount: 100, Enabled: true},
			expected: &endpoint.MetaServiceGroupStatus{AssignmentCount: 100, Enabled: true},
		},
		{
			id:       "group3",
			status:   &endpoint.MetaServiceGroupStatus{AssignmentCount: 99, Enabled: true},
			expected: &endpoint.MetaServiceGroupStatus{AssignmentCount: 99, Enabled: true},
		},
		{
			id:       "group1",
			status:   &endpoint.MetaServiceGroupStatus{AssignmentCount: 0, Enabled: false},
			expected: &endpoint.MetaServiceGroupStatus{AssignmentCount: 0, Enabled: false},
		},
	}
	result := map[string]*endpoint.MetaServiceGroupStatus{
		"group1": {AssignmentCount: 0, Enabled: false},
		"group2": {AssignmentCount: 1, Enabled: false},
		"group3": {AssignmentCount: 99, Enabled: true},
	}
	checkStatus(re, store, map[string]*endpoint.MetaServiceGroupStatus{})
	for testCase := range testCases {
		mustRunTestCase(re, store, testCases[testCase])
	}
	checkStatus(re, store, result)
	// should treat extra groups as having empty disabled status.
	extraGroups := map[string]*endpoint.MetaServiceGroupStatus{
		"group4": {},
		"group5": {},
	}
	checkStatus(re, store, extraGroups)
}
