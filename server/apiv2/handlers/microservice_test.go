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

package handlers

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestHTTPSchemeFromTLS(t *testing.T) {
	re := require.New(t)
	re.Equal("http", httpSchemeFromTLS(nil))
	re.Equal("http", httpSchemeFromTLS(&grpcutil.TLSConfig{}))
	re.Equal("https", httpSchemeFromTLS(&grpcutil.TLSConfig{CertPath: "/path/to/cert.pem"}))
}

func TestResolveProxyURL(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		serviceAddr   string
		defaultScheme string
		expected      string
	}{
		// A registered address without a scheme (a bare advertise-listen-addr)
		// takes the default scheme.
		{"127.0.0.1:3379", "http", "http://127.0.0.1:3379"},
		{"localhost:3379", "http", "http://localhost:3379"},
		{"127.0.0.1:3379", "https", "https://127.0.0.1:3379"},
		// An address that already carries a scheme is kept as-is.
		{"http://127.0.0.1:3379", "http", "http://127.0.0.1:3379"},
		{"https://127.0.0.1:3379", "http", "https://127.0.0.1:3379"},
	}
	for _, tc := range testCases {
		u, err := resolveProxyURL(tc.serviceAddr, tc.defaultScheme)
		re.NoError(err)
		re.Equal(tc.expected, u.String())
	}
}
