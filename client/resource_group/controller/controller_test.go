// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,g
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"testing"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/require"
)

func TestGroupControlBurstable(t *testing.T) {
	re := require.New(t)
	group := &rmpb.ResourceGroup{
		Name: "test",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: 1000,
				},
			},
		},
	}
	ch1 := make(chan struct{})
	ch2 := make(chan *groupCostController)
	gc, err := newGroupCostController(group, DefaultConfig(), ch1, ch2)
	re.NoError(err)
	gc.initRunState()
	args := tokenBucketReconfigureArgs{
		NewRate:  1000,
		NewBurst: -1,
	}
	for _, counter := range gc.run.requestUnitTokens {
		counter.limiter.Reconfigure(time.Now(), args)
	}
	gc.updateAvgRequestResourcePerSec()
	re.Equal(gc.burstable.Load(), true)
}
