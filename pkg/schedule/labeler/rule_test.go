// Copyright 2022 TiKV Project Authors.
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

package labeler

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRegionLabelTTL(t *testing.T) {
	re := require.New(t)
	label := RegionLabel{Key: "k1", Value: "v1"}

	// test label with no ttl.
	err := label.checkAndAdjustExpire()
	re.NoError(err)
	re.Empty(label.StartAt)
	re.Empty(label.expire)

	// test rule with illegal ttl.
	label.TTL = "ttl"
	err = label.checkAndAdjustExpire()
	re.Error(err)

	// test legal rule with ttl
	label.TTL = "10h10m10s10ms"
	err = label.checkAndAdjustExpire()
	re.NoError(err)
	re.NotEmpty(label.StartAt)
	re.False(label.expireBefore(time.Now().Add(time.Hour)))
	re.True(label.expireBefore(time.Now().Add(24 * time.Hour)))

	// test legal rule with ttl, rule unmarshal from json.
	data, err := json.Marshal(label)
	re.NoError(err)
	var label2 RegionLabel
	err = json.Unmarshal(data, &label2)
	re.NoError(err)
	re.Equal(label.StartAt, label2.StartAt)
	re.Equal(label.TTL, label2.TTL)
	label2.checkAndAdjustExpire()
	// The `expire` should be the same with minor inaccuracies.
	re.Less(math.Abs(label2.expire.Sub(*label.expire).Seconds()), 1.0)
}
