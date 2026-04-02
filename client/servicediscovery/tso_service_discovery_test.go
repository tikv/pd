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

package servicediscovery

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/tsopb"
)

func TestKeyspaceGroupSvcDiscoveryUpdateKeepsPrimaryOnStaleRevision(t *testing.T) {
	re := require.New(t)
	originalGroup := &tsopb.KeyspaceGroup{Id: 1}
	newGroup := &tsopb.KeyspaceGroup{Id: 2}
	k := &keyspaceGroupSvcDiscovery{
		group:         originalGroup,
		primaryURL:    "http://primary-1",
		secondaryURLs: []string{"http://secondary-1"},
		urls:          []string{"http://primary-1", "http://secondary-1"},
		modRevision:   atomic.Uint64{},
	}
	k.modRevision.Store(10)

	oldPrimaryURL, primarySwitched, success := k.update(
		newGroup,
		"http://primary-2",
		[]string{"http://secondary-2"},
		[]string{"http://primary-2", "http://secondary-2"},
		9,
	)

	re.Equal("http://primary-1", oldPrimaryURL)
	re.True(primarySwitched)
	re.False(success)
	re.Equal("http://primary-2", k.primaryURL)
	re.Equal([]string{"http://secondary-1"}, k.secondaryURLs)
	re.Equal([]string{"http://primary-1", "http://secondary-1"}, k.urls)
	re.Same(originalGroup, k.group)
	re.Equal(uint64(10), k.getModRevision())
}

func TestKeyspaceGroupSvcDiscoveryUpdateUpdatesAllFieldsOnFreshRevision(t *testing.T) {
	re := require.New(t)
	newGroup := &tsopb.KeyspaceGroup{Id: 2}
	k := &keyspaceGroupSvcDiscovery{
		primaryURL:    "http://primary-1",
		secondaryURLs: []string{"http://secondary-1"},
		urls:          []string{"http://primary-1", "http://secondary-1"},
		modRevision:   atomic.Uint64{},
	}
	k.modRevision.Store(10)

	oldPrimaryURL, primarySwitched, success := k.update(
		newGroup,
		"http://primary-2",
		[]string{"http://secondary-2"},
		[]string{"http://primary-2", "http://secondary-2"},
		11,
	)

	re.Equal("http://primary-1", oldPrimaryURL)
	re.True(primarySwitched)
	re.True(success)
	re.Equal("http://primary-2", k.primaryURL)
	re.Equal([]string{"http://secondary-2"}, k.secondaryURLs)
	re.Equal([]string{"http://primary-2", "http://secondary-2"}, k.urls)
	re.Same(newGroup, k.group)
	re.Equal(uint64(11), k.getModRevision())
}
