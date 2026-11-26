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

package apis

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	mconfig "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"

	rmserver "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestGetConfigRedactsMeteringCredentials(t *testing.T) {
	re := require.New(t)
	const (
		role          = "trn:iam::1:role/metering"
		tosAK         = "test-tos-ak"
		tosSK         = "test-tos-sk"
		tosToken      = "test-tos-token"
		redactedValue = "***REDACTED***"
	)
	gin.SetMode(gin.TestMode)

	cfg := rmserver.NewConfig()
	cfg.Metering = mconfig.MeteringConfig{
		Type:   storage.ProviderTypeTOS,
		Region: "cn-beijing",
		Bucket: "metering",
		TOS: &mconfig.MeteringTOSConfig{
			AssumeRoleARN:   role,
			AccessKey:       tosAK,
			SecretAccessKey: tosSK,
			SessionToken:    tosToken,
		},
	}
	svr := rmserver.CreateServer(t.Context(), cfg)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/resource-manager/api/v1/config", http.NoBody)
	c.Set(multiservicesapi.ServiceContextKey, svr)

	getConfig(c)

	re.Equal(http.StatusOK, recorder.Code)
	body := recorder.Body.String()
	re.NotContains(body, tosAK)
	re.NotContains(body, tosSK)
	re.NotContains(body, tosToken)

	var respCfg rmserver.Config
	re.NoError(json.Unmarshal(recorder.Body.Bytes(), &respCfg))
	re.NotNil(respCfg.Metering.TOS)
	re.Equal(role, respCfg.Metering.TOS.AssumeRoleARN)
	re.Equal(redactedValue, respCfg.Metering.TOS.AccessKey)
	re.Equal(redactedValue, respCfg.Metering.TOS.SecretAccessKey)
	re.Equal(redactedValue, respCfg.Metering.TOS.SessionToken)

	// Live config must retain original credentials for the metering writer.
	liveTOS := svr.GetConfig().Metering.TOS
	re.Equal(tosAK, liveTOS.AccessKey)
	re.Equal(tosSK, liveTOS.SecretAccessKey)
	re.Equal(tosToken, liveTOS.SessionToken)
}
