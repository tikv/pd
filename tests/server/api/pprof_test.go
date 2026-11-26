// Copyright 2021 TiKV Project Authors.
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

package api

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/suite"

	mconfig "github.com/pingcap/metering_sdk/config"

	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

const (
	pprofTOSRole     = "trn:iam::1:role/pprof-metering"
	pprofTOSAK       = "test-tos-ak"
	pprofTOSSK       = "test-tos-sk"
	pprofTOSToken    = "test-tos-token"
	redactedTOSValue = "***REDACTED***"
)

type pprofTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestPprofTestSuite(t *testing.T) {
	suite.Run(t, new(pprofTestSuite))
}

func (suite *pprofTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T(), func(conf *config.Config, _ string) {
		// Leave Type empty so no cloud metering writer starts while exercising config serialization.
		conf.Metering = mconfig.MeteringConfig{
			Region: "cn-beijing",
			Bucket: "metering",
			TOS: &mconfig.MeteringTOSConfig{
				AssumeRoleARN:   pprofTOSRole,
				AccessKey:       pprofTOSAK,
				SecretAccessKey: pprofTOSSK,
				SessionToken:    pprofTOSToken,
			},
		}
	})
}

func (suite *pprofTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *pprofTestSuite) TestGetZip() {
	suite.env.RunTestInNonMicroserviceEnv(suite.checkGetZip)
}

func (suite *pprofTestSuite) checkGetZip(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"

	rsp, err := tests.TestDialClient.Get(urlPrefix + "/debug/pprof/zip?" + "seconds=5")
	re.NoError(err)
	defer rsp.Body.Close()
	body, err := io.ReadAll(rsp.Body)
	re.NoError(err)
	re.NotNil(body)
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	re.NoError(err)
	re.Len(zipReader.File, 7)

	var configBytes []byte
	for _, f := range zipReader.File {
		if f.Name != "config" {
			continue
		}
		rc, err := f.Open()
		re.NoError(err)
		configBytes, err = io.ReadAll(rc)
		re.NoError(rc.Close())
		re.NoError(err)
		break
	}
	re.NotEmpty(configBytes)
	re.NotContains(string(configBytes), pprofTOSAK)
	re.NotContains(string(configBytes), pprofTOSSK)
	re.NotContains(string(configBytes), pprofTOSToken)

	var dumpedCfg config.Config
	re.NoError(json.Unmarshal(configBytes, &dumpedCfg))
	re.NotNil(dumpedCfg.Metering.TOS)
	re.Equal(pprofTOSRole, dumpedCfg.Metering.TOS.AssumeRoleARN)
	re.Equal(redactedTOSValue, dumpedCfg.Metering.TOS.AccessKey)
	re.Equal(redactedTOSValue, dumpedCfg.Metering.TOS.SecretAccessKey)
	re.Equal(redactedTOSValue, dumpedCfg.Metering.TOS.SessionToken)

	liveTOS := leader.GetConfig().Metering.TOS
	re.NotNil(liveTOS)
	re.Equal(pprofTOSAK, liveTOS.AccessKey)
	re.Equal(pprofTOSSK, liveTOS.SecretAccessKey)
	re.Equal(pprofTOSToken, liveTOS.SessionToken)
}
