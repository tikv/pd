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

package storelimit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreLimit(t *testing.T) {
	re := require.New(t)
	rate := int64(15)
	limit := NewStoreRateLimit(float64(rate))
	re.True(limit.Available(influence*rate, AddPeer))
	re.True(limit.Take(influence*rate, AddPeer))
	re.False(limit.Take(influence, AddPeer))

	limit.Reset(float64(rate), AddPeer)
	re.False(limit.Available(influence, AddPeer))
	re.False(limit.Take(influence, AddPeer))

	limit.Reset(0, AddPeer)
	re.True(limit.Available(influence, AddPeer))
	re.True(limit.Take(influence, AddPeer))
}
