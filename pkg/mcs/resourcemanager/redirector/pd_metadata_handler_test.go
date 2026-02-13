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

package redirector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	bs "github.com/tikv/pd/pkg/basicserver"
	pderrors "github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	rmserver "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/metering"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestShouldHandlePDMetadataLocally(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	tests := []struct {
		method string
		path   string
		expect bool
	}{
		{http.MethodPost, "/resource-manager/api/v1/config/group", true},
		{http.MethodPut, "/resource-manager/api/v1/config/group", true},
		{http.MethodGet, "/resource-manager/api/v1/config/groups", true},
		{http.MethodGet, "/resource-manager/api/v1/config/group/test", true},
		{http.MethodDelete, "/resource-manager/api/v1/config/group/test", true},
		{http.MethodPost, "/resource-manager/api/v1/config/keyspace/service-limit", true},
		{http.MethodGet, "/resource-manager/api/v1/config/keyspace/service-limit/test", true},
		{http.MethodGet, "/resource-manager/api/v1/config", false},
		{http.MethodPut, "/resource-manager/api/v1/admin/log", false},
	}
	for _, tc := range tests {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		re.Equal(tc.expect, shouldHandlePDMetadataLocally(req), "method=%s path=%s", tc.method, tc.path)
	}
}

func TestPDMetadataHandlerGroupCRUDAndErrorCodes(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	store := newTestPDMetadataStore()
	handler := newPDMetadataHandlerWithStore(store)

	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
	}

	resp := doJSONRequest(re, handler, http.MethodPost, "/resource-manager/api/v1/config/group", group)
	re.Equal(http.StatusOK, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodGet, "/resource-manager/api/v1/config/group/test_group", nil)
	re.Equal(http.StatusOK, resp.Code)

	group.Priority = 9
	resp = doJSONRequest(re, handler, http.MethodPut, "/resource-manager/api/v1/config/group", group)
	re.Equal(http.StatusOK, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodDelete, "/resource-manager/api/v1/config/group/test_group", nil)
	re.Equal(http.StatusOK, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodGet, "/resource-manager/api/v1/config/group/test_group", nil)
	re.Equal(http.StatusNotFound, resp.Code)

	store.addErr = errors.New("add failed")
	resp = doJSONRequest(re, handler, http.MethodPost, "/resource-manager/api/v1/config/group", group)
	re.Equal(http.StatusInternalServerError, resp.Code)

	resp = doRawRequest(handler, http.MethodPost, "/resource-manager/api/v1/config/group", []byte("{invalid"))
	re.Equal(http.StatusBadRequest, resp.Code)
}

func TestPDMetadataHandlerControllerAndServiceLimit(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	store := newTestPDMetadataStore()
	handler := newPDMetadataHandlerWithStore(store)

	resp := doJSONRequest(re, handler, http.MethodPost, "/resource-manager/api/v1/config/controller", map[string]any{
		"unknown": 1,
	})
	re.Equal(http.StatusBadRequest, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodPost,
		"/resource-manager/api/v1/config/keyspace/service-limit/path_keyspace?keyspace_name=query_keyspace",
		map[string]float64{"service_limit": 12.5})
	re.Equal(http.StatusOK, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodGet, "/resource-manager/api/v1/config/keyspace/service-limit/path_keyspace", nil)
	re.Equal(http.StatusOK, resp.Code)
	re.Equal(12.5, readServiceLimit(re, resp))

	resp = doJSONRequest(re, handler, http.MethodGet, "/resource-manager/api/v1/config/keyspace/service-limit/query_keyspace", nil)
	re.Equal(http.StatusNotFound, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodPost,
		"/resource-manager/api/v1/config/keyspace/service-limit?keyspace_name=query_keyspace",
		map[string]float64{"service_limit": 7.5})
	re.Equal(http.StatusOK, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodGet,
		"/resource-manager/api/v1/config/keyspace/service-limit?keyspace_name=query_keyspace", nil)
	re.Equal(http.StatusOK, resp.Code)
	re.Equal(7.5, readServiceLimit(re, resp))

	resp = doJSONRequest(re, handler, http.MethodPost,
		"/resource-manager/api/v1/config/keyspace/service-limit/non-existing",
		map[string]float64{"service_limit": 1})
	re.Equal(http.StatusBadRequest, resp.Code)

	store.setServiceLimitErr = errors.New("set service-limit failed")
	resp = doJSONRequest(re, handler, http.MethodPost,
		"/resource-manager/api/v1/config/keyspace/service-limit/path_keyspace",
		map[string]float64{"service_limit": 1})
	re.Equal(http.StatusBadRequest, resp.Code)
}

func TestNewPDMetadataHandler(t *testing.T) {
	t.Parallel()

	provider := &mockManagerProvider{}
	manager := rmserver.NewManager[*mockManagerProvider](provider)
	handler := newPDMetadataHandler(manager)
	require.NotNil(t, handler)
}

type mockManagerProvider struct{ bs.Server }

func (*mockManagerProvider) GetControllerConfig() *rmserver.ControllerConfig {
	return &rmserver.ControllerConfig{}
}

func (*mockManagerProvider) GetMeteringWriter() *metering.Writer { return nil }

func (*mockManagerProvider) AddStartCallback(...func()) {}

func (*mockManagerProvider) AddServiceReadyCallback(...func(context.Context) error) {}

type testPDMetadataStore struct {
	keyspaceIDs        map[string]uint32
	validKeyspaceIDs   map[uint32]struct{}
	groups             map[string]*rmserver.ResourceGroup
	serviceLimits      map[uint32]float64
	addErr             error
	setServiceLimitErr error
}

func newTestPDMetadataStore() *testPDMetadataStore {
	keyspaceIDs := map[string]uint32{
		"":               constant.NullKeyspaceID,
		"path_keyspace":  1,
		"query_keyspace": 2,
	}
	validKeyspaceIDs := make(map[uint32]struct{}, len(keyspaceIDs))
	for _, id := range keyspaceIDs {
		validKeyspaceIDs[id] = struct{}{}
	}
	return &testPDMetadataStore{
		keyspaceIDs:      keyspaceIDs,
		validKeyspaceIDs: validKeyspaceIDs,
		groups:           make(map[string]*rmserver.ResourceGroup),
		serviceLimits:    make(map[uint32]float64),
	}
}

func groupKey(keyspaceID uint32, name string) string {
	return fmt.Sprintf("%d/%s", keyspaceID, name)
}

func (s *testPDMetadataStore) AddResourceGroup(group *rmpb.ResourceGroup) error {
	if s.addErr != nil {
		return s.addErr
	}
	keyspaceID := rmserver.ExtractKeyspaceID(group.GetKeyspaceId())
	s.groups[groupKey(keyspaceID, group.GetName())] = rmserver.FromProtoResourceGroup(group)
	return nil
}

func (s *testPDMetadataStore) ModifyResourceGroup(group *rmpb.ResourceGroup) error {
	keyspaceID := rmserver.ExtractKeyspaceID(group.GetKeyspaceId())
	key := groupKey(keyspaceID, group.GetName())
	old, ok := s.groups[key]
	if !ok {
		return pderrors.ErrResourceGroupNotExists.FastGenByArgs(group.GetName())
	}
	return old.PatchSettings(group)
}

func (s *testPDMetadataStore) GetResourceGroup(keyspaceID uint32, name string, withStats bool) (*rmserver.ResourceGroup, error) {
	group, ok := s.groups[groupKey(keyspaceID, name)]
	if !ok {
		return nil, pderrors.ErrResourceGroupNotExists.FastGenByArgs(name)
	}
	return group.Clone(withStats), nil
}

func (*testPDMetadataStore) GetResourceGroupList(_ uint32, _ bool) ([]*rmserver.ResourceGroup, error) {
	return []*rmserver.ResourceGroup{}, nil
}

func (s *testPDMetadataStore) DeleteResourceGroup(keyspaceID uint32, name string) error {
	key := groupKey(keyspaceID, name)
	if _, ok := s.groups[key]; !ok {
		return pderrors.ErrResourceGroupNotExists.FastGenByArgs(name)
	}
	delete(s.groups, key)
	return nil
}

func (*testPDMetadataStore) GetControllerConfig() *rmserver.ControllerConfig {
	return &rmserver.ControllerConfig{}
}

func (*testPDMetadataStore) UpdateControllerConfigItem(_ string, _ any) error {
	return nil
}

func (s *testPDMetadataStore) SetKeyspaceServiceLimit(keyspaceID uint32, serviceLimit float64) error {
	if s.setServiceLimitErr != nil {
		return s.setServiceLimitErr
	}
	if _, ok := s.validKeyspaceIDs[keyspaceID]; !ok {
		return pderrors.ErrKeyspaceNotExists.FastGenByArgs(keyspaceID)
	}
	s.serviceLimits[keyspaceID] = serviceLimit
	return nil
}

func (s *testPDMetadataStore) lookupKeyspaceID(_ context.Context, name string) (uint32, error) {
	keyspaceID, ok := s.keyspaceIDs[name]
	if !ok {
		return 0, pderrors.ErrKeyspaceNotExists.FastGenByArgs(name)
	}
	return keyspaceID, nil
}

func (s *testPDMetadataStore) lookupKeyspaceServiceLimit(keyspaceID uint32) (any, bool) {
	serviceLimit, ok := s.serviceLimits[keyspaceID]
	if !ok {
		return nil, false
	}
	return map[string]float64{"service_limit": serviceLimit}, true
}

func readServiceLimit(re *require.Assertions, resp *httptest.ResponseRecorder) float64 {
	var out struct {
		ServiceLimit float64 `json:"service_limit"`
	}
	re.NoError(json.Unmarshal(resp.Body.Bytes(), &out))
	return out.ServiceLimit
}

func doJSONRequest(re *require.Assertions, handler http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	var reqBody *bytes.Buffer
	if body != nil {
		data, err := json.Marshal(body)
		re.NoError(err)
		reqBody = bytes.NewBuffer(data)
	} else {
		reqBody = bytes.NewBuffer(nil)
	}
	req := httptest.NewRequest(method, path, reqBody)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	return resp
}

func doRawRequest(handler http.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	return resp
}
