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

package command_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/utils/apiutil"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

type transferCommandResult struct {
	output           string
	membersRequests  int32
	readyRequests    int32
	transferRequests int32
}

func runTransferCommand(
	t *testing.T,
	readyStatus int,
	input string,
	additionalArgs ...string,
) transferCommandResult {
	return runTransferCommandForMember(t, readyStatus, input, "pd2", false, additionalArgs...)
}

func runTransferCommandForMember(
	t *testing.T,
	readyStatus int,
	input string,
	memberName string,
	targetIsLeader bool,
	additionalArgs ...string,
) transferCommandResult {
	t.Helper()

	var readyRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/pd/api/v2/ready" || r.Method != http.MethodGet {
			t.Errorf("unexpected readiness request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get(apiutil.PDAllowFollowerHandleHeader); got != "true" {
			t.Errorf("expected %s header to be true, got %q", apiutil.PDAllowFollowerHandleHeader, got)
		}
		readyRequests.Add(1)
		w.WriteHeader(readyStatus)
	}))
	t.Cleanup(target.Close)

	var membersRequests atomic.Int32
	var transferRequests atomic.Int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/pd/api/v1/members":
			membersRequests.Add(1)
			members := &pdpb.GetMembersResponse{
				Members: []*pdpb.Member{{
					Name:       "pd2",
					ClientUrls: []string{target.URL},
				}},
			}
			if targetIsLeader {
				members.EtcdLeader = members.Members[0]
			}
			if err := json.NewEncoder(w).Encode(members); err != nil {
				t.Errorf("encode members response: %v", err)
			}
		case "/pd/api/v1/leader/transfer/" + memberName:
			if r.Method != http.MethodPost {
				t.Errorf("expected POST transfer request, got %s", r.Method)
			}
			transferRequests.Add(1)
		default:
			t.Errorf("unexpected control request path %q", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(control.Close)

	cmd := ctl.GetRootCmd()
	var output bytes.Buffer
	cmd.SetIn(strings.NewReader(input))
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	args := []string{"-u", control.URL, "member", "leader", "transfer", memberName}
	cmd.SetArgs(append(args, additionalArgs...))
	require.NoError(t, cmd.Execute())

	return transferCommandResult{
		output:           output.String(),
		membersRequests:  membersRequests.Load(),
		readyRequests:    readyRequests.Load(),
		transferRequests: transferRequests.Load(),
	}
}

func TestTransferPDLeaderChecksTargetReadiness(t *testing.T) {
	result := runTransferCommand(t, http.StatusOK, "")
	require.Equal(t, int32(1), result.membersRequests)
	require.Equal(t, int32(1), result.readyRequests)
	require.Equal(t, int32(1), result.transferRequests)
	require.Contains(t, result.output, "Success!")
}

func TestTransferPDLeaderRequiresExactConfirmation(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
	}{
		{name: "declined", input: "N\n"},
		{name: "lowercase", input: "y\n"},
		{name: "extra input", input: "Y yes\n"},
		{name: "empty input", input: "\n"},
		{name: "EOF"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := runTransferCommand(t, http.StatusInternalServerError, test.input)
			require.Equal(t, int32(1), result.readyRequests)
			require.Equal(t, int32(0), result.transferRequests)
			require.Contains(t, result.output, "may cause the PD leader to be unable to serve requests for an extended period")
			require.Contains(t, result.output, "Enter Y to continue")
			require.Contains(t, result.output, "Leader transfer aborted")
		})
	}
}

func TestTransferPDLeaderContinuesAfterExplicitConfirmation(t *testing.T) {
	result := runTransferCommand(t, http.StatusInternalServerError, "Y\n")
	require.Equal(t, int32(1), result.readyRequests)
	require.Equal(t, int32(1), result.transferRequests)
	require.Contains(t, result.output, "Success!")
}

func TestTransferPDLeaderForceSkipsReadinessPreflight(t *testing.T) {
	for _, forceFlag := range []string{"--force", "-f"} {
		t.Run(forceFlag, func(t *testing.T) {
			result := runTransferCommand(t, http.StatusInternalServerError, "", forceFlag)
			require.Equal(t, int32(0), result.membersRequests)
			require.Equal(t, int32(0), result.readyRequests)
			require.Equal(t, int32(1), result.transferRequests)
			require.NotContains(t, result.output, "Enter Y to continue")
			require.Contains(t, result.output, "Success!")
		})
	}
}

func TestTransferPDLeaderRejectsUnknownMember(t *testing.T) {
	result := runTransferCommandForMember(t, http.StatusOK, "", "missing", false)
	require.Equal(t, int32(1), result.membersRequests)
	require.Equal(t, int32(0), result.readyRequests)
	require.Equal(t, int32(0), result.transferRequests)
	require.Contains(t, result.output, `target PD member "missing" was not found`)
	require.NotContains(t, result.output, "Enter Y to continue")
}

func TestTransferPDLeaderStillSubmitsWhenTargetWasLeader(t *testing.T) {
	// The leader can change after the member list is returned, so the transfer must still be submitted.
	result := runTransferCommandForMember(t, http.StatusOK, "", "pd2", true)
	require.Equal(t, int32(1), result.membersRequests)
	require.Equal(t, int32(1), result.readyRequests)
	require.Equal(t, int32(1), result.transferRequests)
	require.Contains(t, result.output, "Success!")
}
