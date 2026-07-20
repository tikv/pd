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

package join

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestCanonicalizeURL(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		in   string
		want string
	}{
		{"http://pd.example:2379", "http://pd.example:2379"},
		{"http://PD.EXAMPLE:2379", "http://pd.example:2379"},                                   // case-insensitive host
		{"HTTP://pd.example:2379", "http://pd.example:2379"},                                   // case-insensitive scheme
		{"http://pd.example.:2379", "http://pd.example:2379"},                                  // trailing DNS dot
		{" http://pd.example:2379 ", "http://pd.example:2379"},                                 // surrounding spaces
		{"http://[2001:0db8:0000:0000:0000:0000:0000:0001]:2379", "http://[2001:db8::1]:2379"}, // IPv6 canonical form
		{"http://[2001:DB8::1]:2379", "http://[2001:db8::1]:2379"},                             // IPv6 case
		{"https://10.0.0.1:2379", "https://10.0.0.1:2379"},
		{"not a url", "not a url"}, // unparsable, returned as-is (trimmed)
	}
	for _, tc := range testCases {
		re.Equal(tc.want, canonicalizeURL(tc.in), tc.in)
	}
}

func TestCheckClientURLConflict(t *testing.T) {
	re := require.New(t)
	members := []*etcdserverpb.Member{
		{
			ID:         1,
			Name:       "pd1",
			PeerURLs:   []string{"http://10.0.0.1:2380"},
			ClientURLs: []string{"http://pd.example:2379"},
		},
		{
			ID:         2,
			Name:       "pd2",
			PeerURLs:   []string{"http://10.0.0.2:2380"},
			ClientURLs: []string{"http://[2001:db8::1]:2379"},
		},
	}
	testCases := []struct {
		name       string
		clientURLs []string
		peerURLs   []string
		conflict   bool
	}{
		{"exact duplicate", []string{"http://pd.example:2379"}, []string{"http://10.0.0.9:2380"}, true},
		{"case-insensitive host", []string{"http://PD.EXAMPLE:2379"}, []string{"http://10.0.0.9:2380"}, true},
		{"case-insensitive scheme", []string{"HTTP://pd.example:2379"}, []string{"http://10.0.0.9:2380"}, true},
		{"trailing dot", []string{"http://pd.example.:2379"}, []string{"http://10.0.0.9:2380"}, true},
		{"ipv6 equivalent", []string{"http://[2001:0db8:0000::0001]:2379"}, []string{"http://10.0.0.9:2380"}, true},
		{"different host", []string{"http://pd.other:2379"}, []string{"http://10.0.0.9:2380"}, false},
		{"different port", []string{"http://pd.example:2380"}, []string{"http://10.0.0.9:2380"}, false},
		// The member's own entry is skipped (matched by peer URL, canonicalized),
		// so its unchanged client URL is not a self-conflict.
		{"self excluded by peer url", []string{"http://pd.example:2379"}, []string{"http://10.0.0.1:2380"}, false},
		{"self excluded case-insensitive peer", []string{"http://pd.example:2379"}, []string{"http://10.0.0.1:2380/"}, false},
	}
	for _, tc := range testCases {
		err := checkClientURLConflict(tc.clientURLs, tc.peerURLs, members)
		if tc.conflict {
			re.Error(err, tc.name)
		} else {
			re.NoError(err, tc.name)
		}
	}
}
