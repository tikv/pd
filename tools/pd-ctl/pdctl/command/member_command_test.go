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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/utils/apiutil"
)

const (
	mockPDURL    = "http://mock-pd:2379"
	targetPDURL  = "http://target-pd:2379"
	targetPDName = "pd2"
)

type transferLeaderRoundTripper struct {
	memberName      string
	clientURLs      []string
	readyErrors     []error
	readyStatusCode int
	targetIsLeader  bool

	membersRequests  int
	readyRequests    int
	transferRequests int
	readyHosts       []string
	readyTimeouts    []time.Duration
	allowFollower    string
	transferHost     string
}

func (rt *transferLeaderRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	statusCode := http.StatusOK
	body := ""
	switch {
	case req.Method == http.MethodGet && req.URL.Path == "/"+membersPrefix:
		rt.membersRequests++
		clientURLs := rt.clientURLs
		if len(clientURLs) == 0 {
			clientURLs = []string{targetPDURL}
		}
		members := &pdpb.GetMembersResponse{
			Members: []*pdpb.Member{{
				Name:       targetPDName,
				ClientUrls: clientURLs,
			}},
		}
		if rt.targetIsLeader {
			members.EtcdLeader = members.Members[0]
		}
		data, err := json.Marshal(members)
		if err != nil {
			return nil, err
		}
		body = string(data)
	case req.Method == http.MethodGet && req.URL.Path == "/"+readyPrefix:
		rt.readyRequests++
		rt.readyHosts = append(rt.readyHosts, req.URL.Host)
		if deadline, ok := req.Context().Deadline(); ok {
			rt.readyTimeouts = append(rt.readyTimeouts, time.Until(deadline))
		}
		rt.allowFollower = req.Header.Get(apiutil.PDAllowFollowerHandleHeader)
		requestIndex := rt.readyRequests - 1
		if requestIndex < len(rt.readyErrors) && rt.readyErrors[requestIndex] != nil {
			return nil, rt.readyErrors[requestIndex]
		}
		statusCode = rt.readyStatusCode
	case req.Method == http.MethodPost && req.URL.Path == "/"+leaderMemberPrefix+"/transfer/"+rt.memberName:
		rt.transferRequests++
		rt.transferHost = req.URL.Host
	default:
		statusCode = http.StatusNotFound
	}

	return &http.Response{
		StatusCode: statusCode,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

func executeTransferLeaderCommand(
	re *require.Assertions,
	rt *transferLeaderRoundTripper,
	input string,
	additionalArgs ...string,
) string {
	oldClient := dialClient
	dialClient = &http.Client{Transport: rt}
	defer func() { dialClient = oldClient }()

	cmd := NewMemberCommand()
	cmd.PersistentFlags().String("pd", mockPDURL, "")
	cmd.SetIn(strings.NewReader(input))
	cmd.SetArgs(append([]string{"leader", "transfer", rt.memberName}, additionalArgs...))
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	re.NoError(cmd.Execute())
	return out.String()
}

func TestTransferPDLeaderSubmitsWhenTargetIsReportedAsLeader(t *testing.T) {
	re := require.New(t)
	rt := &transferLeaderRoundTripper{
		memberName:      targetPDName,
		readyStatusCode: http.StatusOK,
		targetIsLeader:  true,
	}

	output := executeTransferLeaderCommand(re, rt, "")
	re.Equal(1, rt.membersRequests)
	re.Equal(1, rt.readyRequests)
	re.Equal([]string{"target-pd:2379"}, rt.readyHosts)
	re.Equal("true", rt.allowFollower)
	re.Equal(1, rt.transferRequests)
	re.Equal("mock-pd:2379", rt.transferHost)
	re.Contains(output, "Success!")
}

func TestTransferPDLeaderRequiresExactConfirmation(t *testing.T) {
	re := require.New(t)

	rt := &transferLeaderRoundTripper{memberName: targetPDName, readyStatusCode: http.StatusInternalServerError}
	output := executeTransferLeaderCommand(re, rt, "y\n")
	re.Equal(0, rt.transferRequests)
	re.Contains(output, "may cause the PD leader to be unable to serve requests for an extended period")
	re.Contains(output, "Enter Y to continue")
	re.Contains(output, "Leader transfer aborted")

	rt = &transferLeaderRoundTripper{memberName: targetPDName, readyStatusCode: http.StatusInternalServerError}
	output = executeTransferLeaderCommand(re, rt, "Y\n")
	re.Equal(1, rt.transferRequests)
	re.Contains(output, "Success!")
}

func TestCheckPDMemberReadyTriesNextURLAfterTimeout(t *testing.T) {
	re := require.New(t)
	rt := &transferLeaderRoundTripper{
		memberName:      targetPDName,
		clientURLs:      []string{"http://target-pd-1:2379", "http://target-pd-2:2379"},
		readyErrors:     []error{context.DeadlineExceeded},
		readyStatusCode: http.StatusOK,
	}
	oldClient := dialClient
	dialClient = &http.Client{Transport: rt}
	defer func() { dialClient = oldClient }()

	cmd := NewMemberCommand()
	cmd.Flags().String("pd", mockPDURL, "")
	cmd.SetContext(t.Context())

	found, err := checkPDMemberReady(cmd, targetPDName)
	re.NoError(err)
	re.True(found)
	re.Equal([]string{"target-pd-1:2379", "target-pd-2:2379"}, rt.readyHosts)
	re.Len(rt.readyTimeouts, 2)
	for _, timeout := range rt.readyTimeouts {
		re.Greater(timeout, readyRequestTimeout-time.Second)
		re.LessOrEqual(timeout, readyRequestTimeout)
	}
}

func TestCheckPDMemberReadyTriesNextURLAfterInvalidURL(t *testing.T) {
	re := require.New(t)
	rt := &transferLeaderRoundTripper{
		memberName:      targetPDName,
		clientURLs:      []string{"http://[::1", "http://target-pd-2:2379"},
		readyStatusCode: http.StatusOK,
	}
	oldClient := dialClient
	dialClient = &http.Client{Transport: rt}
	defer func() { dialClient = oldClient }()

	cmd := NewMemberCommand()
	cmd.Flags().String("pd", mockPDURL, "")
	cmd.SetContext(t.Context())

	found, err := checkPDMemberReady(cmd, targetPDName)
	re.NoError(err)
	re.True(found)
	re.Equal([]string{"target-pd-2:2379"}, rt.readyHosts)
}

func TestTransferPDLeaderForceSkipsReadinessPreflight(t *testing.T) {
	re := require.New(t)

	for _, forceFlag := range []string{"--force", "-f"} {
		rt := &transferLeaderRoundTripper{memberName: targetPDName}
		executeTransferLeaderCommand(re, rt, "", forceFlag)
		re.Equal(0, rt.membersRequests, forceFlag)
		re.Equal(0, rt.readyRequests, forceFlag)
		re.Equal(1, rt.transferRequests, forceFlag)
	}
}

func TestTransferPDLeaderRejectsUnknownMember(t *testing.T) {
	re := require.New(t)
	rt := &transferLeaderRoundTripper{memberName: "missing", readyStatusCode: http.StatusOK}

	output := executeTransferLeaderCommand(re, rt, "")
	re.Equal(1, rt.membersRequests)
	re.Equal(0, rt.readyRequests)
	re.Equal(0, rt.transferRequests)
	re.Contains(output, `target PD member "missing" was not found`)
}
