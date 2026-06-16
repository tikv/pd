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

package grpcutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/client/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestGetCallerID(t *testing.T) {
	re := require.New(t)
	tests := []struct {
		name          string
		advertiseAddr string
		expectedHost  string
	}{
		{
			name:          "valid http address",
			advertiseAddr: "http://127.0.0.1:2379",
			expectedHost:  "127.0.0.1:2379",
		},
		{
			name:          "valid https address",
			advertiseAddr: "https://127.0.0.1:2379",
			expectedHost:  "127.0.0.1:2379",
		},
		{
			name:          "valid address without scheme",
			advertiseAddr: "localhost:2379",
			expectedHost:  "localhost:2379",
		},
		{
			name:          "empty address",
			advertiseAddr: "",
			expectedHost:  "",
		},
		{
			name:          "valid IPv4 address without scheme",
			advertiseAddr: "127.0.0.1:2379",
			expectedHost:  "127.0.0.1:2379",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			actual := GetCalleeID(tt.advertiseAddr)
			re.Equal(tt.expectedHost, actual)
		})
	}
}
