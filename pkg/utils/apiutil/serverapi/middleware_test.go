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

package serverapi

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

// Ensure when a rule matches but no primary address is available, we still mark it matched
// so the caller can return ErrRedirect (expected by API behavior).
func TestMatchMicroserviceRedirectNoPrimary(t *testing.T) {
	re := require.New(t)
	req, err := http.NewRequest(http.MethodPost, "/pd/api/v1/admin/reset-ts", http.NoBody)
	re.NoError(err)

	matched, addr := MatchMicroserviceRedirect(
		req,
		[]RedirectRule{{
			MatchPath:         "/pd/api/v1/admin/reset-ts",
			TargetPath:        "/tso/api/v1/admin/reset-ts",
			TargetServiceName: constant.TSOServiceName,
			MatchMethods:      []string{http.MethodPost},
		}},
		true,                              /* isKeyspaceGroupEnabled */
		func(string) bool { return true }, /* isServiceIndependent */
		func(_ context.Context, _ string) (string, bool) { return "", false }, /* getPrimary */
	)
	re.True(matched)
	re.Empty(addr)
}

func TestMatchMicroserviceRedirectForbiddenHeader(t *testing.T) {
	re := require.New(t)
	req, err := http.NewRequest(http.MethodGet, "/pd/api/v1/foo", http.NoBody)
	re.NoError(err)
	req.Header.Set(apiutil.XForbiddenForwardToMicroserviceHeader, "true")

	matched, addr := MatchMicroserviceRedirect(
		req,
		[]RedirectRule{{
			MatchPath:         "/pd/api/v1/foo",
			TargetPath:        "/target",
			TargetServiceName: constant.SchedulingServiceName,
			MatchMethods:      []string{http.MethodGet},
		}},
		true,
		func(string) bool { return true },
		func(_ context.Context, _ string) (string, bool) { return "addr", true },
	)

	re.False(matched)
	re.Empty(addr)
}

func TestMatchMicroserviceRedirectKeyspaceDisabled(t *testing.T) {
	re := require.New(t)
	req, err := http.NewRequest(http.MethodGet, "/pd/api/v1/foo", http.NoBody)
	re.NoError(err)

	matched, addr := MatchMicroserviceRedirect(
		req,
		[]RedirectRule{{
			MatchPath:         "/pd/api/v1/foo",
			TargetPath:        "/target",
			TargetServiceName: constant.SchedulingServiceName,
			MatchMethods:      []string{http.MethodGet},
		}},
		false, // keyspace group disabled
		func(string) bool { return true },
		func(_ context.Context, _ string) (string, bool) { return "addr", true },
	)

	re.False(matched)
	re.Empty(addr)
}

func TestMatchMicroserviceRedirectSchedulingNotIndependent(t *testing.T) {
	re := require.New(t)
	req, err := http.NewRequest(http.MethodGet, "/pd/api/v1/foo", http.NoBody)
	re.NoError(err)

	matched, addr := MatchMicroserviceRedirect(
		req,
		[]RedirectRule{{
			MatchPath:         "/pd/api/v1/foo",
			TargetPath:        "/target",
			TargetServiceName: constant.SchedulingServiceName,
			MatchMethods:      []string{http.MethodGet},
		}},
		true,
		func(name string) bool { return name != constant.SchedulingServiceName },
		func(_ context.Context, _ string) (string, bool) { return "addr", true },
	)

	re.False(matched)
	re.Empty(addr)
}

func TestMatchMicroserviceRedirectFilterBlocks(t *testing.T) {
	re := require.New(t)
	req, err := http.NewRequest(http.MethodGet, "/pd/api/v1/foo", http.NoBody)
	re.NoError(err)

	matched, addr := MatchMicroserviceRedirect(
		req,
		[]RedirectRule{{
			MatchPath:         "/pd/api/v1/foo",
			TargetPath:        "/target",
			TargetServiceName: constant.SchedulingServiceName,
			MatchMethods:      []string{http.MethodGet},
			Filter: func(_ *http.Request) bool {
				return false
			},
		}},
		true,
		func(string) bool { return true },
		func(_ context.Context, _ string) (string, bool) { return "addr", true },
	)

	re.False(matched)
	re.Empty(addr)
}
