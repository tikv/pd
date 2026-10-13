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

package middlewares

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	scheapi "github.com/tikv/pd/pkg/mcs/scheduling/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/apiutil/serverapi"
)

func TestAffinityMicroserviceRedirectRules(t *testing.T) {
	targetAffinityPath := scheapi.APIPathPrefix + "/affinity-groups"
	tests := []struct {
		name       string
		method     string
		path       string
		wantMatch  bool
		wantTarget string
	}{
		{
			name:       "list affinity groups",
			method:     http.MethodGet,
			path:       pdAffinityPath,
			wantMatch:  true,
			wantTarget: targetAffinityPath,
		},
		{
			name:       "get affinity group",
			method:     http.MethodGet,
			path:       pdAffinityPath + "/_tidb_t_42",
			wantMatch:  true,
			wantTarget: targetAffinityPath + "/_tidb_t_42",
		},
		{
			name:   "create affinity groups remains on PD",
			method: http.MethodPost,
			path:   pdAffinityPath,
		},
		{
			name:       "update affinity policy",
			method:     http.MethodPatch,
			path:       pdAffinityPath + "/_tidb_t_42/policy",
			wantMatch:  true,
			wantTarget: targetAffinityPath + "/_tidb_t_42/policy",
		},
		{
			name:   "batch affinity patch remains on PD",
			method: http.MethodPatch,
			path:   pdAffinityPath,
		},
		{
			name:   "other affinity post remains on PD",
			method: http.MethodPost,
			path:   pdAffinityPath + "/_tidb_t_42",
		},
		{
			name:       "rebalance affinity group",
			method:     http.MethodPost,
			path:       pdAffinityPath + "/_tidb_t_42/rebalance",
			wantMatch:  true,
			wantTarget: targetAffinityPath + "/_tidb_t_42/rebalance",
		},
		{
			name:   "rebalance requires one group path segment",
			method: http.MethodPost,
			path:   pdAffinityPath + "/_tidb_t_42/extra/rebalance",
		},
		{
			name:   "rebalance requires a group id",
			method: http.MethodPost,
			path:   pdAffinityPath + "//rebalance",
		},
		{
			name:   "similar prefix remains on PD",
			method: http.MethodPost,
			path:   pdAffinityPath + "x/_tidb_t_42/rebalance",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			req, err := http.NewRequest(tc.method, tc.path, http.NoBody)
			re.NoError(err)
			matched, addr := serverapi.MatchMicroserviceRedirect(
				req,
				affinityMicroserviceRedirectRules(),
				true,
				func(string) bool { return true },
				func(context.Context, string) (string, bool) { return "http://scheduling", true },
			)
			re.Equal(tc.wantMatch, matched)
			if !tc.wantMatch {
				re.Empty(addr)
				return
			}
			re.Equal("http://scheduling", addr)
			re.Equal(tc.wantTarget, req.URL.Path)
		})
	}
}

func TestAffinityMicroserviceRedirectUsesSchedulingService(t *testing.T) {
	re := require.New(t)
	rules := affinityMicroserviceRedirectRules()
	re.Len(rules, 2)
	for _, rule := range rules {
		re.Equal(constant.SchedulingServiceName, rule.TargetServiceName)
	}
}
