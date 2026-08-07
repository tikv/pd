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

package metadataapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	//nolint:staticcheck // kvproto is generated against the legacy protobuf runtime.
	"github.com/golang/protobuf/jsonpb"
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

func TestConfigServiceGroupCRUDAndErrorCodes(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	store := newTestStore()
	handler := newTestHTTPHandler(store)

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

	resp := doJSONRequest(re, handler, http.MethodPut, "/resource-manager/api/v1/config/group", group)
	re.Equal(http.StatusNotFound, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodPost, "/resource-manager/api/v1/config/group", group)
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

	legacyKeyspaceIDs := []struct {
		name       string
		keyspaceID uint32
		body       []byte
	}{
		{"legacy_default_group", 0, []byte(`{"name":"legacy_default_group","keyspace_id":{}}`)},
		{"legacy_camel_default_group", 0, []byte(`{"name":"legacy_camel_default_group","keyspaceId":{}}`)},
		{"legacy_uppercase_group", 42, []byte(`{"name":"legacy_uppercase_group","KEYSPACE_ID":{"VALUE":42}}`)},
	}
	for _, legacyKeyspaceID := range legacyKeyspaceIDs {
		resp = doRawResourceGroupRequest(handler, http.MethodPost, legacyKeyspaceID.body)
		re.Equal(http.StatusOK, resp.Code)
		re.Contains(store.groups, groupKey(legacyKeyspaceID.keyspaceID, legacyKeyspaceID.name))
		resp = doRawResourceGroupRequest(handler, http.MethodPut, legacyKeyspaceID.body)
		re.Equal(http.StatusOK, resp.Code)
	}

	legacyFieldsBody := []byte(`{
		"name":"legacy_fields_group",
		"mode":1,
		"PRIORITY":7,
		"r_u_settings":{"R_U":{"SETTINGS":{"FILL_RATE":123,"BURST_LIMIT":456}}},
		"keyspace_id":{"value":42}
	}`)
	resp = doRawResourceGroupRequest(handler, http.MethodPost, legacyFieldsBody)
	re.Equal(http.StatusOK, resp.Code)
	legacyFieldsGroup := store.groups[groupKey(42, "legacy_fields_group")]
	re.NotNil(legacyFieldsGroup)
	re.Equal(uint32(7), legacyFieldsGroup.Priority)
	re.NotNil(legacyFieldsGroup.RUSettings)
	re.NotNil(legacyFieldsGroup.RUSettings.RU)
	re.NotNil(legacyFieldsGroup.RUSettings.RU.Settings)
	re.Equal(uint64(123), legacyFieldsGroup.RUSettings.RU.Settings.FillRate)
	re.Equal(int64(456), legacyFieldsGroup.RUSettings.RU.Settings.BurstLimit)

	legacyFieldsBody = []byte(`{
		"name":"legacy_fields_group",
		"mode":1,
		"PRIORITY":9,
		"r_u_settings":{"R_U":{"SETTINGS":{"FILL_RATE":321,"BURST_LIMIT":654}}},
		"keyspace_id":{"value":42}
	}`)
	resp = doRawResourceGroupRequest(handler, http.MethodPut, legacyFieldsBody)
	re.Equal(http.StatusOK, resp.Code)
	re.Equal(uint32(9), store.groups[groupKey(42, "legacy_fields_group")].Priority)
	re.Equal(uint64(321), store.groups[groupKey(42, "legacy_fields_group")].RUSettings.RU.Settings.FillRate)
	re.Equal(int64(654), store.groups[groupKey(42, "legacy_fields_group")].RUSettings.RU.Settings.BurstLimit)

	protobufJSONGroup := &rmpb.ResourceGroup{
		Name:       "protobuf_json_group",
		Mode:       rmpb.GroupMode_RUMode,
		Priority:   11,
		KeyspaceId: &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: 42}},
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   789,
					BurstLimit: 987,
				},
			},
		},
	}
	var protobufJSON bytes.Buffer
	re.NoError((&jsonpb.Marshaler{}).Marshal(&protobufJSON, protobufJSONGroup))
	re.Contains(protobufJSON.String(), `"mode":"RUMode"`)
	re.Contains(protobufJSON.String(), `"fillRate":"789"`)
	re.Contains(protobufJSON.String(), `"burstLimit":"987"`)
	for _, method := range []string{http.MethodPost, http.MethodPut} {
		resp = doRawResourceGroupRequest(handler, method, protobufJSON.Bytes())
		re.Equal(http.StatusOK, resp.Code, resp.Body.String())
		storedGroup := store.groups[groupKey(42, protobufJSONGroup.Name)]
		re.NotNil(storedGroup)
		re.Equal(rmpb.GroupMode_RUMode, storedGroup.Mode)
		re.Equal(uint32(11), storedGroup.Priority)
		re.Equal(uint64(789), storedGroup.RUSettings.RU.Settings.FillRate)
		re.Equal(int64(987), storedGroup.RUSettings.RU.Settings.BurstLimit)
	}

	store.addErr = errors.New("add failed")
	resp = doJSONRequest(re, handler, http.MethodPost, "/resource-manager/api/v1/config/group", group)
	re.Equal(http.StatusInternalServerError, resp.Code)
	store.addErr = nil

	resp = doRawResourceGroupRequest(handler, http.MethodPost, []byte("{invalid"))
	re.Equal(http.StatusBadRequest, resp.Code)

	invalidKeyspaceIDs := []struct {
		body    []byte
		message string
	}{
		{
			[]byte(`{"name":"test_group","keyspace_id":{"Keyspace":{"value":42}}}`),
			"keyspace_id must contain a legacy value",
		},
		{
			[]byte(`{"name":"test_group","keyspace_id":{"keyspace_identity":{"namespace_id":1,"keyspace_id":42}}}`),
			"keyspace_id must contain a legacy value",
		},
		{
			[]byte(`{"name":"test_group","keyspace_id":{},"keyspaceId":{"keyspace_identity":{"namespace_id":1,"keyspace_id":42}}}`),
			"keyspace_id must be set only once",
		},
		{
			[]byte(`{"name":"test_group","keyspaceId":{"keyspace_identity":{"namespace_id":1,"keyspace_id":42}},"keyspace_id":{}}`),
			"keyspace_id must be set only once",
		},
		{
			[]byte(`{"name":"test_group","keyspace_id":{"keyspace_identity":{"namespace_id":1,"keyspace_id":42}},"keyspace_id":{}}`),
			"keyspace_id must be set only once",
		},
		{
			[]byte(`{"name":"test_group","KEYSPACE_ID":{},"keyspaceId":{"value":42}}`),
			"keyspace_id must be set only once",
		},
		{
			[]byte(`{"name":"test_group","mode":"RUMode","keyspaceId":{"keyspaceIdentity":{"namespaceId":1,"keyspaceId":42}}}`),
			"keyspace_id must contain a legacy value",
		},
	}
	for _, invalidKeyspaceID := range invalidKeyspaceIDs {
		for _, method := range []string{http.MethodPost, http.MethodPut} {
			resp = doRawResourceGroupRequest(handler, method, invalidKeyspaceID.body)
			re.Equal(http.StatusBadRequest, resp.Code)
			re.Contains(resp.Body.String(), invalidKeyspaceID.message)
		}
	}

	mixedJSONDialects := []byte(`{"name":"mixed_json_group","mode":"RUMode","PRIORITY":7}`)
	for _, method := range []string{http.MethodPost, http.MethodPut} {
		resp = doRawResourceGroupRequest(handler, method, mixedJSONDialects)
		re.Equal(http.StatusBadRequest, resp.Code)
		re.NotContains(store.groups, groupKey(constant.NullKeyspaceID, "mixed_json_group"))
	}

	oversizedBody := bytes.Repeat([]byte("x"), int(maxResourceGroupRequestBytes)+1)
	for _, method := range []string{http.MethodPost, http.MethodPut} {
		resp = doRawResourceGroupRequest(handler, method, oversizedBody)
		re.Equal(http.StatusRequestEntityTooLarge, resp.Code)
		re.Contains(resp.Body.String(), "request body too large")
	}
}

// TestConfigServiceLoadingReturns503 asserts the "resource groups are still
// being loaded" error is reported as retryable. It is a transient startup state,
// not an internal error, so callers and load balancers must be able to tell the
// difference and retry.
func TestConfigServiceLoadingReturns503(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	store := newTestStore()
	handler := newTestHTTPHandler(store)

	store.listErr = pderrors.ErrResourceGroupsLoading
	resp := doJSONRequest(re, handler, http.MethodGet, "/resource-manager/api/v1/config/groups", nil)
	re.Equal(http.StatusServiceUnavailable, resp.Code)

	store.listErr = errors.New("boom")
	resp = doJSONRequest(re, handler, http.MethodGet, "/resource-manager/api/v1/config/groups", nil)
	re.Equal(http.StatusInternalServerError, resp.Code)
}

func TestConfigServiceControllerAllOrNothing(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	store := newTestStore()
	handler := newTestHTTPHandler(store)

	resp := doJSONRequest(re, handler, http.MethodPost, "/resource-manager/api/v1/config/controller", map[string]any{
		"enable-controller-trace-log": true,
		"unknown":                     1,
	})
	re.Equal(http.StatusBadRequest, resp.Code)
	re.Empty(store.updatedControllerConfigItems)

	resp = doJSONRequest(re, handler, http.MethodPost, "/resource-manager/api/v1/config/controller", map[string]any{
		"enable-controller-trace-log": true,
	})
	re.Equal(http.StatusOK, resp.Code)
	re.Len(store.updatedControllerConfigItems, 1)
}

func TestConfigServiceKeyspaceServiceLimitAndErrors(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	store := newTestStore()
	handler := newTestHTTPHandler(store)

	resp := doJSONRequest(re, handler, http.MethodPost,
		"/resource-manager/api/v1/config/keyspace/service-limit/path_keyspace",
		map[string]float64{"service_limit": 12.5})
	re.Equal(http.StatusOK, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodGet, "/resource-manager/api/v1/config/keyspace/service-limit/path_keyspace", nil)
	re.Equal(http.StatusOK, resp.Code)
	re.Equal(12.5, readServiceLimit(re, resp))

	resp = doJSONRequest(re, handler, http.MethodPost,
		"/resource-manager/api/v1/config/keyspace/service-limit/non-existing",
		map[string]float64{"service_limit": 1})
	re.Equal(http.StatusNotFound, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodGet,
		"/resource-manager/api/v1/config/keyspace/service-limit/non-existing", nil)
	re.Equal(http.StatusNotFound, resp.Code)

	store.setServiceLimitErr = errors.New("set service-limit failed")
	resp = doJSONRequest(re, handler, http.MethodPost,
		"/resource-manager/api/v1/config/keyspace/service-limit/path_keyspace",
		map[string]float64{"service_limit": 1})
	re.Equal(http.StatusInternalServerError, resp.Code)

	store.setServiceLimitErr = nil
	resp = doJSONRequest(re, handler, http.MethodPost,
		"/resource-manager/api/v1/config/keyspace/service-limit/path_keyspace",
		map[string]float64{"service_limit": -1})
	re.Equal(http.StatusBadRequest, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodGet,
		"/resource-manager/api/v1/config/keyspace/service-limit/not_created", nil)
	re.Equal(http.StatusNotFound, resp.Code)
}

func TestConfigServiceMetadataWriteDisabledReturns403(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	manager := rmserver.NewManager[*tokenOnlyManagerProvider](&tokenOnlyManagerProvider{})
	handler := newTestHTTPHandler(NewManagerStore(manager))

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
	re.Equal(http.StatusForbidden, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodPost, "/resource-manager/api/v1/config/controller", map[string]any{
		"enable-controller-trace-log": true,
	})
	re.Equal(http.StatusForbidden, resp.Code)
}

func newTestHTTPHandler(configStore ConfigStore) http.Handler {
	engine := gin.New()
	engine.Use(gin.Recovery())
	root := engine.Group("/resource-manager/api/v1/")
	configEndpoint := root.Group("/config")
	NewConfigService(configStore).Register(configEndpoint)
	return engine
}

type tokenOnlyManagerProvider struct{ bs.Server }

func (*tokenOnlyManagerProvider) GetControllerConfig() *rmserver.ControllerConfig {
	return &rmserver.ControllerConfig{}
}

func (*tokenOnlyManagerProvider) GetMeteringWriter() *metering.Writer { return nil }

func (*tokenOnlyManagerProvider) GetResourceGroupWriteRole() rmserver.ResourceGroupWriteRole {
	return rmserver.ResourceGroupWriteRoleRMTokenOnly
}

func (*tokenOnlyManagerProvider) AddStartCallback(...func()) {}

func (*tokenOnlyManagerProvider) AddServiceReadyCallback(...func(context.Context) error) {}

type testStore struct {
	keyspaceIDs                  map[string]uint32
	validKeyspaceIDs             map[uint32]struct{}
	groups                       map[string]*rmserver.ResourceGroup
	serviceLimits                map[uint32]float64
	addErr                       error
	setServiceLimitErr           error
	listErr                      error
	updatedControllerConfigItems []string
}

func newTestStore() *testStore {
	keyspaceIDs := map[string]uint32{
		"":              constant.NullKeyspaceID,
		"path_keyspace": 1,
		"not_created":   3,
	}
	validKeyspaceIDs := make(map[uint32]struct{}, len(keyspaceIDs)-1)
	for name, id := range keyspaceIDs {
		if name == "not_created" {
			continue
		}
		validKeyspaceIDs[id] = struct{}{}
	}
	return &testStore{
		keyspaceIDs:      keyspaceIDs,
		validKeyspaceIDs: validKeyspaceIDs,
		groups:           make(map[string]*rmserver.ResourceGroup),
		serviceLimits:    make(map[uint32]float64),
	}
}

func groupKey(keyspaceID uint32, name string) string {
	return fmt.Sprintf("%d/%s", keyspaceID, name)
}

func (s *testStore) AddResourceGroup(group *rmpb.ResourceGroup) error {
	if s.addErr != nil {
		return s.addErr
	}
	keyspaceID := rmserver.ExtractKeyspaceID(group.GetKeyspaceId())
	s.groups[groupKey(keyspaceID, group.GetName())] = rmserver.FromProtoResourceGroup(group)
	return nil
}

func (s *testStore) ModifyResourceGroup(group *rmpb.ResourceGroup) error {
	keyspaceID := rmserver.ExtractKeyspaceID(group.GetKeyspaceId())
	key := groupKey(keyspaceID, group.GetName())
	old, ok := s.groups[key]
	if !ok {
		return pderrors.ErrResourceGroupNotExists.FastGenByArgs(group.GetName())
	}
	return old.PatchSettings(group)
}

func (s *testStore) GetResourceGroup(keyspaceID uint32, name string, withStats bool) (*rmserver.ResourceGroup, error) {
	group, ok := s.groups[groupKey(keyspaceID, name)]
	if !ok {
		return nil, pderrors.ErrResourceGroupNotExists.FastGenByArgs(name)
	}
	return group.Clone(withStats), nil
}

func (s *testStore) GetResourceGroupList(_ uint32, _ bool) ([]*rmserver.ResourceGroup, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return []*rmserver.ResourceGroup{}, nil
}

func (s *testStore) DeleteResourceGroup(keyspaceID uint32, name string) error {
	key := groupKey(keyspaceID, name)
	if _, ok := s.groups[key]; !ok {
		return pderrors.ErrResourceGroupNotExists.FastGenByArgs(name)
	}
	delete(s.groups, key)
	return nil
}

func (*testStore) GetControllerConfig() *rmserver.ControllerConfig {
	return &rmserver.ControllerConfig{}
}

func (s *testStore) UpdateControllerConfigItem(key string, _ any) error {
	s.updatedControllerConfigItems = append(s.updatedControllerConfigItems, key)
	return nil
}

func (s *testStore) SetKeyspaceServiceLimit(keyspaceID uint32, serviceLimit float64) error {
	if s.setServiceLimitErr != nil {
		return s.setServiceLimitErr
	}
	if _, ok := s.validKeyspaceIDs[keyspaceID]; !ok {
		return pderrors.ErrKeyspaceNotExists.FastGenByArgs(keyspaceID)
	}
	s.serviceLimits[keyspaceID] = serviceLimit
	return nil
}

func (s *testStore) LookupKeyspaceID(_ context.Context, name string) (uint32, error) {
	keyspaceID, ok := s.keyspaceIDs[name]
	if !ok {
		return 0, pderrors.ErrKeyspaceNotExistsByName.FastGenByArgs(name)
	}
	return keyspaceID, nil
}

func (s *testStore) LookupKeyspaceServiceLimit(keyspaceID uint32) (any, bool) {
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

func doRawResourceGroupRequest(handler http.Handler, method string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/resource-manager/api/v1/config/group", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	return resp
}
