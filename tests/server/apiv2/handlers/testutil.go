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

package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
)

const (
	keyspacesPrefix      = "/pd/api/v2/keyspaces"
	keyspaceGroupsPrefix = "/pd/api/v2/tso/keyspace-groups"
)

func sendLoadRangeRequest(re *require.Assertions, server *tests.TestServer, token, limit string) *handlers.LoadAllKeyspacesResponse {
	// Construct load range request.
	httpReq, err := http.NewRequest(http.MethodGet, server.GetAddr()+keyspacesPrefix, http.NoBody)
	re.NoError(err)
	query := httpReq.URL.Query()
	query.Add("page_token", token)
	query.Add("limit", limit)
	httpReq.URL.RawQuery = query.Encode()
	// Send request.
	httpResp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer httpResp.Body.Close()
	re.Equal(http.StatusOK, httpResp.StatusCode)
	// Receive & decode response.
	data, err := io.ReadAll(httpResp.Body)
	re.NoError(err)
	resp := &handlers.LoadAllKeyspacesResponse{}
	re.NoError(json.Unmarshal(data, resp))
	return resp
}

func sendUpdateStateRequest(re *require.Assertions, server *tests.TestServer, name string, request *handlers.UpdateStateParam) (bool, *keyspacepb.KeyspaceMeta) {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPut, server.GetAddr()+keyspacesPrefix+"/"+name+"/state", bytes.NewBuffer(data))
	re.NoError(err)
	httpResp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		return false, nil
	}
	data, err = io.ReadAll(httpResp.Body)
	re.NoError(err)
	meta := &handlers.KeyspaceMeta{}
	re.NoError(json.Unmarshal(data, meta))
	return true, meta.KeyspaceMeta
}

// MustCreateKeyspace creates a keyspace with HTTP API.
func MustCreateKeyspace(re *require.Assertions, server *tests.TestServer, request *handlers.CreateKeyspaceParams) *keyspacepb.KeyspaceMeta {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, server.GetAddr()+keyspacesPrefix, bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	meta := &handlers.KeyspaceMeta{}
	re.NoError(json.Unmarshal(data, meta))
	checkCreateRequest(re, request, meta.KeyspaceMeta)
	return meta.KeyspaceMeta
}

// checkCreateRequest verifies a keyspace meta matches a create request.
func checkCreateRequest(re *require.Assertions, request *handlers.CreateKeyspaceParams, meta *keyspacepb.KeyspaceMeta) {
	re.Equal(request.Name, meta.Name)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, meta.State)
	re.Equal(request.Config, meta.Config)
}

func mustUpdateKeyspaceConfig(re *require.Assertions, server *tests.TestServer, name string, request *handlers.UpdateConfigParams) *keyspacepb.KeyspaceMeta {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPatch, server.GetAddr()+keyspacesPrefix+"/"+name+"/config", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	meta := &handlers.KeyspaceMeta{}
	re.NoError(json.Unmarshal(data, meta))
	return meta.KeyspaceMeta
}

func mustLoadKeyspaces(re *require.Assertions, server *tests.TestServer, name string) *keyspacepb.KeyspaceMeta {
	resp, err := tests.TestDialClient.Get(server.GetAddr() + keyspacesPrefix + "/" + name)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	data, err := io.ReadAll(resp.Body)
	re.NoError(err)
	meta := &handlers.KeyspaceMeta{}
	re.NoError(json.Unmarshal(data, meta))
	return meta.KeyspaceMeta
}

// MustLoadKeyspaceGroups loads all keyspace groups from the server.
func MustLoadKeyspaceGroups(re *require.Assertions, server *tests.TestServer, token, limit string) []*endpoint.KeyspaceGroup {
	// Construct load range request.
	httpReq, err := http.NewRequest(http.MethodGet, server.GetAddr()+keyspaceGroupsPrefix, http.NoBody)
	re.NoError(err)
	query := httpReq.URL.Query()
	query.Add("page_token", token)
	query.Add("limit", limit)
	httpReq.URL.RawQuery = query.Encode()
	// Send request.
	httpResp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer httpResp.Body.Close()
	data, err := io.ReadAll(httpResp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, httpResp.StatusCode, string(data))
	var resp []*endpoint.KeyspaceGroup
	re.NoError(json.Unmarshal(data, &resp))
	return resp
}

func tryCreateKeyspaceGroup(re *require.Assertions, server *tests.TestServer, request *handlers.CreateKeyspaceGroupParams) (int, string) {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, server.GetAddr()+keyspaceGroupsPrefix, bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	return resp.StatusCode, string(data)
}

// MustLoadKeyspaceGroupByID loads the keyspace group by ID with HTTP API.
func MustLoadKeyspaceGroupByID(re *require.Assertions, server *tests.TestServer, id uint32) *endpoint.KeyspaceGroup {
	var (
		kg   *endpoint.KeyspaceGroup
		code int
	)
	testutil.Eventually(re, func() bool {
		kg, code = TryLoadKeyspaceGroupByID(re, server, id)
		return code == http.StatusOK
	})
	return kg
}

// TryLoadKeyspaceGroupByID loads the keyspace group by ID with HTTP API.
func TryLoadKeyspaceGroupByID(re *require.Assertions, server *tests.TestServer, id uint32) (*endpoint.KeyspaceGroup, int) {
	httpReq, err := http.NewRequest(http.MethodGet, server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d", id), http.NoBody)
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	re.NoError(err)
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode
	}

	var kg endpoint.KeyspaceGroup
	re.NoError(json.Unmarshal(data, &kg))
	return &kg, resp.StatusCode
}

// MustCreateKeyspaceGroup creates a keyspace group with HTTP API.
func MustCreateKeyspaceGroup(re *require.Assertions, server *tests.TestServer, request *handlers.CreateKeyspaceGroupParams) {
	code, data := tryCreateKeyspaceGroup(re, server, request)
	re.Equal(http.StatusOK, code, data)
}

// FailCreateKeyspaceGroupWithCode fails to create a keyspace group with HTTP API.
func FailCreateKeyspaceGroupWithCode(re *require.Assertions, server *tests.TestServer, request *handlers.CreateKeyspaceGroupParams, expect int) {
	code, data := tryCreateKeyspaceGroup(re, server, request)
	re.Equal(expect, code, data)
}

// MustDeleteKeyspaceGroup deletes a keyspace group with HTTP API.
func MustDeleteKeyspaceGroup(re *require.Assertions, server *tests.TestServer, id uint32) {
	httpReq, err := http.NewRequest(http.MethodDelete, server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d", id), http.NoBody)
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode, string(data))
}

// MustSplitKeyspaceGroup splits a keyspace group with HTTP API.
func MustSplitKeyspaceGroup(re *require.Assertions, server *tests.TestServer, id uint32, request *handlers.SplitKeyspaceGroupByIDParams) {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d/split", id), bytes.NewBuffer(data))
	re.NoError(err)
	// Send request.
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode, string(data))
}

// MustFinishSplitKeyspaceGroup finishes a keyspace group split with HTTP API.
func MustFinishSplitKeyspaceGroup(re *require.Assertions, server *tests.TestServer, id uint32) {
	testutil.Eventually(re, func() bool {
		httpReq, err := http.NewRequest(http.MethodDelete, server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d/split", id), http.NoBody)
		if err != nil {
			return false
		}
		// Send request.
		resp, err := tests.TestDialClient.Do(httpReq)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return false
		}
		if resp.StatusCode == http.StatusServiceUnavailable ||
			resp.StatusCode == http.StatusInternalServerError {
			return false
		}
		re.Equal(http.StatusOK, resp.StatusCode, string(data))
		return true
	})
}

// MustMergeKeyspaceGroup merges keyspace groups with HTTP API.
func MustMergeKeyspaceGroup(re *require.Assertions, server *tests.TestServer, id uint32, request *handlers.MergeKeyspaceGroupsParams) {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d/merge", id), bytes.NewBuffer(data))
	re.NoError(err)
	// Send request.
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode, string(data))
}
