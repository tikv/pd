// Copyright 2020 TiKV Project Authors.
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

package encryption

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/typeutil"
)

func TestAdjustDefaultValue(t *testing.T) {
	config := &Config{}
	err := config.Adjust()
	require.NoError(t, err)
	require.Equal(t, methodPlaintext, config.DataEncryptionMethod)
	defaultRotationPeriod, _ := time.ParseDuration(defaultDataKeyRotationPeriod)
	require.Equal(t, defaultRotationPeriod, config.DataKeyRotationPeriod.Duration)
	require.Equal(t, masterKeyTypePlaintext, config.MasterKey.Type)
}

func TestAdjustInvalidDataEncryptionMethod(t *testing.T) {
	config := &Config{DataEncryptionMethod: "unknown"}
	require.NotNil(t, config.Adjust())
}

func TestAdjustNegativeRotationDuration(t *testing.T) {
	config := &Config{DataKeyRotationPeriod: typeutil.NewDuration(time.Duration(int64(-1)))}
	require.NotNil(t, config.Adjust())
}

func TestAdjustInvalidMasterKeyType(t *testing.T) {
	config := &Config{MasterKey: MasterKeyConfig{Type: "unknown"}}
	require.NotNil(t, config.Adjust())
}
