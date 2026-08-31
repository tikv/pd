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

package command

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

type storeLimitTestCase struct {
	name            string
	kernelType      string
	statusCode      int
	args            []string
	expectedOutput  string
	expectedStatus  int
	expectedUpdates int
}

func TestStoreLimitAllKernelAwareMaximum(t *testing.T) {
	testCases := []storeLimitTestCase{
		{
			name:            "classic boundary does not query status",
			args:            []string{"all", "200", "add-peer"},
			expectedUpdates: 1,
		},
		{
			name:           "classic rejects above boundary",
			kernelType:     kerneltype.Classic,
			args:           []string{"all", "201", "add-peer"},
			expectedOutput: "rate should be at most 200.0 for all",
			expectedStatus: 1,
		},
		{
			name:            "nextgen accepts boundary",
			kernelType:      kerneltype.NextGen,
			args:            []string{"all", "1000", "add-peer"},
			expectedStatus:  1,
			expectedUpdates: 1,
		},
		{
			name:            "nextgen label filter accepts boundary",
			kernelType:      kerneltype.NextGen,
			args:            []string{"all", "engine", "tikv", "1000", "remove-peer"},
			expectedStatus:  1,
			expectedUpdates: 1,
		},
		{
			name:           "nextgen rejects above boundary",
			kernelType:     kerneltype.NextGen,
			args:           []string{"all", "1001", "add-peer"},
			expectedOutput: "rate should be at most 1000.0 for all",
			expectedStatus: 1,
		},
		{
			name:           "missing kernel type falls back to classic",
			args:           []string{"all", "201", "add-peer"},
			expectedOutput: "rate should be at most 200.0 for all",
			expectedStatus: 1,
		},
		{
			name:           "status failure fails closed",
			statusCode:     http.StatusInternalServerError,
			args:           []string{"all", "201", "add-peer"},
			expectedOutput: "Failed to get PD kernel type",
			expectedStatus: 1,
		},
	}

	runStoreLimitTestCases(t, NewStoreLimitCommand, testCases)
}

func TestDeprecatedStoreLimitAllKernelAwareMaximum(t *testing.T) {
	testCases := []storeLimitTestCase{
		{
			name:            "classic boundary",
			args:            []string{"200", "add-peer"},
			expectedUpdates: 1,
		},
		{
			name:           "classic rejects above boundary",
			kernelType:     kerneltype.Classic,
			args:           []string{"201", "add-peer"},
			expectedOutput: "rate should be at most 200.0 for all",
			expectedStatus: 1,
		},
		{
			name:            "nextgen boundary",
			kernelType:      kerneltype.NextGen,
			args:            []string{"1000", "remove-peer"},
			expectedStatus:  1,
			expectedUpdates: 1,
		},
		{
			name:           "nextgen rejects above boundary",
			kernelType:     kerneltype.NextGen,
			args:           []string{"1001", "remove-peer"},
			expectedOutput: "rate should be at most 1000.0 for all",
			expectedStatus: 1,
		},
	}

	runStoreLimitTestCases(t, NewSetAllLimitCommand, testCases)
}

func runStoreLimitTestCases(t *testing.T, newCommand func() *cobra.Command, testCases []storeLimitTestCase) {
	t.Helper()
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server, statusRequests, updates := newStoreLimitTestServer(t, testCase.kernelType, testCase.statusCode)
			defer server.Close()

			cmd := newCommand()
			cmd.Flags().String("pd", server.URL, "")
			cmd.SetArgs(testCase.args)
			output := executeStoreLimitCommand(t, cmd)
			require.Equal(t, testCase.expectedStatus, *statusRequests)
			require.Equal(t, testCase.expectedUpdates, *updates)
			if testCase.expectedOutput != "" {
				require.Contains(t, output, testCase.expectedOutput)
			}
		})
	}
}

func newStoreLimitTestServer(t *testing.T, kernelType string, statusCode int) (*httptest.Server, *int, *int) {
	t.Helper()
	statusRequests := 0
	updates := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/" + pdStatusPrefix:
			statusRequests++
			if statusCode != 0 {
				http.Error(w, "status unavailable", statusCode)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if _, err := fmt.Fprintf(w, `{"kernel_type":%q}`, kernelType); err != nil {
				t.Errorf("write status response: %v", err)
			}
		case "/" + storesLimitPrefix:
			updates++
			w.Header().Set("Content-Type", "application/json")
			if _, err := w.Write([]byte(`"Set store limit successfully."`)); err != nil {
				t.Errorf("write store-limit response: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	return server, &statusRequests, &updates
}

func executeStoreLimitCommand(t *testing.T, cmd *cobra.Command) string {
	t.Helper()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	require.NoError(t, cmd.Execute())
	return strings.TrimSpace(output.String())
}
