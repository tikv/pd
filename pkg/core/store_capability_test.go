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

package core

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
)

func TestStoreCapabilityAdmission(t *testing.T) {
	var missing *StoreInfo
	require.False(t, missing.SupportsPeerCapability(false))
	require.False(t, missing.SupportsPeerCapability(true))
	for _, tc := range []struct {
		capability    string
		full, witness bool
	}{
		{"", true, true}, {FullStoreCapability, true, false},
		{LogOnlyStoreCapability, false, true}, {"log-only-v2", false, false},
	} {
		t.Run(tc.capability, func(t *testing.T) {
			labels := map[string]string{}
			if tc.capability != "" {
				labels[StoreCapabilityLabel] = tc.capability
			}
			store := NewStoreInfoWithLabel(1, labels)
			require.Equal(t, tc.full, store.SupportsPeerCapability(false))
			require.Equal(t, tc.witness, store.SupportsPeerCapability(true))
		})
	}
	_, err := ParseStoreCapability(&metapb.Store{Labels: []*metapb.StoreLabel{
		{Key: StoreCapabilityLabel, Value: FullStoreCapability},
		{Key: "CSE-STORE-CAPABILITY", Value: FullStoreCapability},
	}})
	require.ErrorContains(t, err, "duplicate")
}
