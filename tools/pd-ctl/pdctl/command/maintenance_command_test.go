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

package command

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockRoundTripper implements http.RoundTripper for testing
// It returns a canned response for any request

type mockRoundTripper struct {
	resp *http.Response
	err  error
}

func (m *mockRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	return m.resp, m.err
}

func TestMaintenanceSetCommand_Success(t *testing.T) {
	re := require.New(t)
	// Mock response
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("Maintenance task started successfully.")),
	}
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{resp: resp}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceSetCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"tikv", "task1", "--desc=desc"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, "Maintenance task started successfully")
}

func TestMaintenanceSetCommand_Error(t *testing.T) {
	re := require.New(t)
	// Mock error
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{err: errors.New("mock error")}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceSetCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"tikv", "task1"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, "Failed to start maintenance task:")
	re.Contains(result, "mock error")
}

func TestMaintenanceDeleteCommand_Success(t *testing.T) {
	re := require.New(t)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("Maintenance task deleted successfully.")),
	}
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{resp: resp}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceDeleteCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"tikv", "task1"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, "Maintenance task deleted successfully")
}

func TestMaintenanceDeleteCommand_Error(t *testing.T) {
	re := require.New(t)
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{err: errors.New("mock error")}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceDeleteCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"tikv", "task1"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, "Failed to delete maintenance task:")
	re.Contains(result, "mock error")
}

func TestMaintenanceShowCommand_Success(t *testing.T) {
	re := require.New(t)
	// Mock response for success case
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"type":"tikv","id":"task1","start_timestamp":1234567890,"description":"rolling restart for TiKV store-1"}`)),
	}
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{resp: resp}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceShowCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()

	// Check that the raw JSON response is printed
	re.Contains(result, `{"type":"tikv","id":"task1","start_timestamp":1234567890,"description":"rolling restart for TiKV store-1"}`)
}

func TestMaintenanceShowCommand_Error(t *testing.T) {
	re := require.New(t)
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{err: errors.New("mock error")}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceShowCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, "Failed to get maintenance task:")
	re.Contains(result, "mock error")
}

func TestMaintenanceShowCommand_AllTasks(t *testing.T) {
	re := require.New(t)
	// Mock response for all tasks (single task response since only one can run at a time)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"type":"tikv","id":"task1","start_timestamp":1234567890,"description":"rolling restart for TiKV store-1"}`)),
	}
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{resp: resp}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceShowCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, `{"type":"tikv","id":"task1","start_timestamp":1234567890,"description":"rolling restart for TiKV store-1"}`)
}

func TestMaintenanceShowCommand_SpecificTaskType(t *testing.T) {
	re := require.New(t)
	// Mock response for specific task type
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"type":"tikv","id":"task1","start_timestamp":1234567890,"description":"specific task"}`)),
	}
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{resp: resp}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceShowCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"tikv"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, `{"type":"tikv","id":"task1","start_timestamp":1234567890,"description":"specific task"}`)
}

func TestMaintenanceShowCommand_NotFound_AllTasks(t *testing.T) {
	re := require.New(t)
	// Mock 404 response
	resp := &http.Response{
		StatusCode: http.StatusNotFound,
		Body:       io.NopCloser(strings.NewReader("No maintenance tasks are currently running.")),
	}
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{resp: resp}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceShowCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, "No maintenance tasks are currently running.")
}

func TestMaintenanceShowCommand_NotFound_SpecificType(t *testing.T) {
	re := require.New(t)
	// Mock 404 response
	resp := &http.Response{
		StatusCode: http.StatusNotFound,
		Body:       io.NopCloser(strings.NewReader("No maintenance task found for type: tikv")),
	}
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{resp: resp}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceShowCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"tikv"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, "No maintenance task found for type: tikv")
}

func TestMaintenanceShowCommand_InvalidJSON(t *testing.T) {
	re := require.New(t)
	// Mock response with invalid JSON
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("invalid json response")),
	}
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{resp: resp}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceShowCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, "invalid json response")
}

func TestMaintenanceShowCommand_NetworkError(t *testing.T) {
	re := require.New(t)
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{err: errors.New("network error")}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceShowCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, "Failed to get maintenance task:")
	re.Contains(result, "network error")
}

// errorReader implements io.Reader and always returns an error
type errorReader struct{}

func (*errorReader) Read(_ []byte) (n int, err error) {
	return 0, errors.New("read error")
}

func TestMaintenanceShowCommand_ReadBodyError(t *testing.T) {
	re := require.New(t)
	// Mock response with error reader
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(&errorReader{}),
	}
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{resp: resp}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceShowCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, "Failed to get maintenance task:")
	re.Contains(result, "read error")
}

func TestMaintenanceSetCommand_Conflict(t *testing.T) {
	re := require.New(t)
	// Mock 409 conflict response
	resp := &http.Response{
		StatusCode: http.StatusConflict,
		Body:       io.NopCloser(strings.NewReader(`{"error":"Another maintenance task is already running","existing_task":{"type":"tikv","id":"task1","start_timestamp":1234567890,"description":"existing task"}}`)),
	}
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{resp: resp}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceSetCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"tikv", "task2"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, "Failed to start maintenance task:")
	re.Contains(result, "[409]")
}

func TestMaintenanceDeleteCommand_NotFound(t *testing.T) {
	re := require.New(t)
	// Mock 404 response
	resp := &http.Response{
		StatusCode: http.StatusNotFound,
		Body:       io.NopCloser(strings.NewReader("No maintenance task is running for type tikv")),
	}
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{resp: resp}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceDeleteCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"tikv", "task1"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, "Failed to delete maintenance task:")
	re.Contains(result, "[404]")
}

func TestMaintenanceDeleteCommand_Conflict(t *testing.T) {
	re := require.New(t)
	// Mock 409 conflict response
	resp := &http.Response{
		StatusCode: http.StatusConflict,
		Body:       io.NopCloser(strings.NewReader(`{"error":"Task ID does not match the current task","existing_task":{"type":"tikv","id":"task1","start_timestamp":1234567890,"description":"current task"}}`)),
	}
	oldClient := dialClient
	dialClient = &http.Client{Transport: &mockRoundTripper{resp: resp}}
	defer func() { dialClient = oldClient }()

	cmd := newMaintenanceDeleteCommand()
	cmd.Flags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"tikv", "wrong_task_id"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.Execute()
	result := out.String()
	re.Contains(result, "Failed to delete maintenance task:")
	re.Contains(result, "[409]")
}
