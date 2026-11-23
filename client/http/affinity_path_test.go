// Copyright 2023 TiKV Project Authors.
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

package http

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAffinityGroupByIDFormat(t *testing.T) {
	require.Equal(t, "/pd/api/v2/affinity-groups/group", fmt.Sprintf(AffinityGroupByID, "group"))
}

func TestAffinityGroupPathEscaping(t *testing.T) {
	for _, tc := range []struct{ id, escaped string }{
		{"group", "group"},
		{"g1?force=true", "g1%3Fforce=true"},
		{"../affinity-groups?force=true", "..%2Faffinity-groups%3Fforce=true"},
		{"a/b#c% d", "a%2Fb%23c%25%20d"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			re := require.New(t)
			for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
				for _, force := range []bool{false, true} {
					checked := 0
					transport := NewHTTPClientWithRequestChecker(func(req *http.Request) error {
						checked++
						re.Equal(method, req.Method)
						want := "/pd/api/v2/affinity-groups/" + tc.escaped
						if method == http.MethodDelete && force {
							want += "?force=true"
							re.Equal("true", req.URL.Query().Get("force"))
						} else {
							re.Empty(req.URL.RawQuery)
						}
						re.Equal(want, req.URL.RequestURI())
						return nil
					})
					c := newClientWithMockServiceDiscovery("affinity-path", []string{"http://127.0.0.1"}, WithHTTPClient(transport))
					t.Cleanup(c.Close)
					switch method {
					case http.MethodGet:
						_, err := c.GetAffinityGroup(t.Context(), tc.id)
						re.NoError(err)
					case http.MethodPut:
						_, err := c.UpdateAffinityGroupPeers(t.Context(), tc.id, 1, []uint64{1})
						re.NoError(err)
					case http.MethodDelete:
						re.NoError(c.DeleteAffinityGroup(t.Context(), tc.id, force))
					}
					re.Equal(1, checked)
				}
			}
		})
	}
}

func TestAffinityGroupEmptyID(t *testing.T) {
	re := require.New(t)
	checked := 0
	transport := NewHTTPClientWithRequestChecker(func(_ *http.Request) error { checked++; return nil })
	c := newClientWithMockServiceDiscovery("affinity-empty-id", []string{"http://127.0.0.1"}, WithHTTPClient(transport))
	defer c.Close()
	_, err := c.GetAffinityGroup(t.Context(), "")
	re.ErrorContains(err, "group ID must not be empty")
	_, err = c.UpdateAffinityGroupPeers(t.Context(), "", 1, []uint64{1})
	re.ErrorContains(err, "group ID must not be empty")
	for _, force := range []bool{false, true} {
		re.ErrorContains(c.DeleteAffinityGroup(t.Context(), "", force), "group ID must not be empty")
	}
	re.Zero(checked)
}
