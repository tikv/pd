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

package gc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGCBarrierInfoExpiration(t *testing.T) {
	re := require.New(t)

	now := time.Now()
	b := NewGCBarrierInfo("b", 1, time.Second, now)
	re.False(b.isExpiredImpl(now.Add(-time.Second)))
	re.False(b.isExpiredImpl(now))
	re.False(b.isExpiredImpl(now.Add(time.Millisecond * 999)))
	re.False(b.isExpiredImpl(now.Add(time.Second)))
	re.True(b.isExpiredImpl(now.Add(time.Second + time.Millisecond)))
	re.True(b.isExpiredImpl(now.Add(time.Hour * 24 * 365 * 10)))

	b = NewGCBarrierInfo("b", 1, TTLNeverExpire, now)
	re.False(b.isExpiredImpl(now))
	re.False(b.isExpiredImpl(now.Add(time.Hour * 24 * 365 * 10)))
}
