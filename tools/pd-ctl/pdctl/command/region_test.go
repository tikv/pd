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

// Notes: it's a copy from mok https://github.com/oh-my-tidb/mok

package command

import (
	"bytes"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/tools/pd-ctl/helper/mok"
	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/codec"
)

type captureRoundTripper struct {
	roundTrip func(*http.Request) (*http.Response, error)
}

func (c *captureRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return c.roundTrip(req)
}

func TestCheckKey(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		key     []byte
		isValid bool
		tableID int64
	}{
		{[]byte("748000000000001dffb25f698000000000ff00000c0380000000ff23c1000603800000ff0000000001038000ff000067f52f2d0398ff00000000556d9500fe"), true, 7602}, // new mod
		{[]byte("748000000000001dffb25f698000000000ff00000c0380000000ff245c32c103800000ff0000000006038000ff00006818d60803d8ff00000008c904c300fe"), true, 7602}, // new mod
		{[]byte("7480000000000AE1FFAB5F72F800000000FF052EEA0100000000FB"), false, 713131},                                                                      // end with 0x01
		{[]byte("7480000000000ADEFF9E5F72F800000000FF024C9D0100000000FB"), false, 712350},                                                                      // end with 0x01
		{[]byte("7480000000000B01FFE75F72F800000000FF05613A0100000000FB"), false, 721383},                                                                      // end with 0x01
		{[]byte("7480000000000B01FFE75F72F800000000FF05613A0200000000FB"), false, 721383},                                                                      // end with 0x02
		{[]byte("7480000000000B01FFE75F72F800000000FF05613A0000000000FB"), true, 721383},                                                                       // end with 0x00
		{[]byte("7480000000000B01FFE75F720000000000FA"), true, 721383},
		{[]byte("7480000000000ADEFF9E5F720000000000FA"), true, 712350},
		{[]byte("7480000000000AE1FFAB5F720000000000FA"), true, 713131},
		// TODO: only consider the 9 bytes of the key and with non 0x00
		{[]byte("7480000000000001FFD75F728000000000FF0000140130FF0000FD"), true, 471}, // end with 0x0130FF
	}
	for _, tc := range testCases {
		rootNode := mok.N("key", tc.key)
		rootNode.Expand()
		re.Equal(tc.isValid, !hasSpecialPatternRecursive(rootNode), string(tc.key))
		tableID, found, err := extractTableIDRecursive(rootNode)
		re.NoError(err)
		re.True(found)
		re.Equal(tc.tableID, tableID)
	}
}

func TestExpandHexStackOverflow(t *testing.T) {
	// This is a valid hex string
	hexStr := "616263646566" // "abcdef" in hex
	n := mok.N("key", []byte(hexStr))
	// Due to infinite recursion, this would theoretically cause a stack overflow
	// When running the actual test, use go test -timeout to prevent hanging
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Recovered from panic: %v", r)
		}
	}()
	n.Expand()
	// If no panic occurs, it means there was no stack overflow
}

func TestExpandStackOverflowFromLogKey(t *testing.T) {
	// Key from the logs
	hexStr := "748000000000008cff065f698000000000ff00000b0380000000ff066a7b8603800000ff0000000002013739ff303200000000fb04ff19b6865c37000000ff03c000000000083eff0b00000000000000f8"

	// Decode test
	oriBytes, err := hex.DecodeString(hexStr)
	if err != nil {
		t.Fatalf("Failed to decode hex string: %v", err)
	}

	// Create node
	n := mok.N("key", oriBytes)

	var expandedNode *mok.Node

	// Limit execution time to avoid infinite loop
	done := make(chan bool)
	go func() {
		// Execute Expand
		expandedNode = n.Expand()
		// Mark successful completion
		done <- true
	}()

	// Wait for processing to complete or timeout
	select {
	case <-done:
		// Successfully completed, no stack overflow
		t.Log("Test completed successfully without stack overflow")

		// Print node structure
		t.Log("Node structure:")
		// Use direct method to capture output
		t.Logf("Original key: %s", expandedNode.String())
		t.Logf("First level variants count: %d", len(expandedNode.GetVariants()))
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out after 5 seconds - possible infinite recursion")
	}
}

func TestShowRegionsByKeyspaceTableIDCommandFuncWithKeyspaceID(t *testing.T) {
	re := require.New(t)
	var requestURL *url.URL
	resp := `{"regions":[]}`
	oldClient := dialClient
	dialClient = &http.Client{
		Transport: &captureRoundTripper{
			roundTrip: func(req *http.Request) (*http.Response, error) {
				requestURL = req.URL
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString(resp)),
				}, nil
			},
		},
	}
	defer func() { dialClient = oldClient }()

	cmd := NewRegionsByKeyspaceTableIDCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"17"})
	re.NoError(cmd.Execute())

	re.NotNil(requestURL)
	re.Equal("/pd/api/v1/regions/key", requestURL.Path)
	expectedStartKey := codec.EncodeBytes(nil, []byte{'x', 0, 0, 17})
	expectedEndKey := codec.EncodeBytes(nil, []byte{'x', 0, 0, 18})
	re.Equal(string(expectedStartKey), mustQueryUnescape(t, requestURL.Query().Get("key")))
	re.Equal(string(expectedEndKey), mustQueryUnescape(t, requestURL.Query().Get("end_key")))
	re.Equal(resp+"\n", out.String())
}

func TestShowRegionsByKeyspaceTableIDCommandFuncWithTableIDAndLimit(t *testing.T) {
	re := require.New(t)
	var requestURL *url.URL
	resp := `{"regions":[{"id":1}]}`
	oldClient := dialClient
	dialClient = &http.Client{
		Transport: &captureRoundTripper{
			roundTrip: func(req *http.Request) (*http.Response, error) {
				requestURL = req.URL
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString(resp)),
				}, nil
			},
		},
	}
	defer func() { dialClient = oldClient }()

	cmd := NewRegionsByKeyspaceTableIDCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"17", "table-id", "45", "3"})
	re.NoError(cmd.Execute())

	re.NotNil(requestURL)
	re.Equal("/pd/api/v1/regions/key", requestURL.Path)
	tablePrefix := []byte{'x', 0, 0, 17, 't'}
	startKey := codec.EncodeInt(append([]byte{}, tablePrefix...), 45)
	endKey := codec.EncodeInt(append([]byte{}, tablePrefix...), 46)
	re.Equal(string(codec.EncodeBytes(nil, startKey)), mustQueryUnescape(t, requestURL.Query().Get("key")))
	re.Equal(string(codec.EncodeBytes(nil, endKey)), mustQueryUnescape(t, requestURL.Query().Get("end_key")))
	re.Equal("3", requestURL.Query().Get("limit"))
	re.Equal(resp+"\n", out.String())
}

func TestShowRegionsByKeyspaceTableIDCommandFuncWithInvalidKeyspaceIDRange(t *testing.T) {
	re := require.New(t)

	cmd := NewRegionsByKeyspaceTableIDCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"16777216"})
	re.NoError(cmd.Execute())

	re.Contains(out.String(), "keyspace-id should be a number between 0 and 16777215")
}

func TestShowRegionsByKeyspaceTableIDCommandFuncWithInvalidTableIDKeyword(t *testing.T) {
	re := require.New(t)

	cmd := NewRegionsByKeyspaceTableIDCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"17", "table", "45"})
	re.NoError(cmd.Execute())

	re.Contains(out.String(), "table-id is required")
}

func mustQueryUnescape(t *testing.T, value string) string {
	t.Helper()
	unescaped, err := url.QueryUnescape(value)
	require.NoError(t, err)
	return unescaped
}
