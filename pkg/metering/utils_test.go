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

package metering

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
)

func TestNewRUValueClampsNegativeValues(t *testing.T) {
	re := require.New(t)

	re.Equal(common.MeteringValue{Value: 0, Unit: UnitRU}, NewRUValue(-1))
	re.Equal(common.MeteringValue{Value: 1, Unit: UnitRU}, NewRUValue(1.5))
}

func TestRedactedConfigMasksTOSCredentials(t *testing.T) {
	re := require.New(t)
	const (
		role          = "trn:iam::1:role/metering"
		tosAK         = "test-tos-ak"
		tosSK         = "test-tos-sk"
		tosToken      = "test-tos-token"
		redactedValue = "***REDACTED***"
	)

	// Guard must not depend on Type so an inactive [metering.tos] block is still masked.
	for _, providerType := range []storage.ProviderType{storage.ProviderTypeTOS, ""} {
		original := config.MeteringConfig{
			Type:   providerType,
			Region: "cn-beijing",
			Bucket: "metering",
			TOS: &config.MeteringTOSConfig{
				AssumeRoleARN:   role,
				AccessKey:       tosAK,
				SecretAccessKey: tosSK,
				SessionToken:    tosToken,
			},
		}

		redacted := RedactedConfig(original)

		re.Equal(tosAK, original.TOS.AccessKey)
		re.Equal(tosSK, original.TOS.SecretAccessKey)
		re.Equal(tosToken, original.TOS.SessionToken)

		re.Equal(role, redacted.TOS.AssumeRoleARN)
		re.Equal(redactedValue, redacted.TOS.AccessKey)
		re.Equal(redactedValue, redacted.TOS.SecretAccessKey)
		re.Equal(redactedValue, redacted.TOS.SessionToken)

		body, err := json.Marshal(redacted)
		re.NoError(err)
		encoded := string(body)
		re.NotContains(encoded, tosAK)
		re.NotContains(encoded, tosSK)
		re.NotContains(encoded, tosToken)
		re.Contains(encoded, role)
	}
}

func TestRedactedConfigLeavesEmptyTOSCredentialsEmpty(t *testing.T) {
	re := require.New(t)
	original := config.MeteringConfig{
		Type: storage.ProviderTypeTOS,
		TOS: &config.MeteringTOSConfig{
			AssumeRoleARN: "trn:iam::1:role/metering",
		},
	}

	redacted := RedactedConfig(original)

	re.Empty(redacted.TOS.AccessKey)
	re.Empty(redacted.TOS.SecretAccessKey)
	re.Empty(redacted.TOS.SessionToken)
	re.Equal("trn:iam::1:role/metering", redacted.TOS.AssumeRoleARN)

	body, err := json.Marshal(redacted)
	re.NoError(err)
	re.NotContains(string(body), "access-key")
	re.NotContains(string(body), "secret-access-key")
	re.NotContains(string(body), "session-token")
	re.NotContains(string(body), "***REDACTED***")

	re.Nil(RedactedConfig(config.MeteringConfig{}).TOS)
}
