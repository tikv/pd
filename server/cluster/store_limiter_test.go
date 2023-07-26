// Copyright 2019 TiKV Project Authors.
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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockconfig"
)

func TestStoreLimitScene(t *testing.T) {
	re := require.New(t)

	limiter := NewStoreLimiter(mockconfig.NewTestOptions())
	re.Equal(storelimit.DefaultScene(storelimit.AddPeer), limiter.scene[storelimit.AddPeer])
	re.Equal(storelimit.DefaultScene(storelimit.RemovePeer), limiter.scene[storelimit.RemovePeer])
}

func TestReplaceStoreLimitScene(t *testing.T) {
	re := require.New(t)

	limiter := NewStoreLimiter(mockconfig.NewTestOptions())

	sceneAddPeer := &storelimit.Scene{Idle: 4, Low: 3, Normal: 2, High: 1}
	limiter.ReplaceStoreLimitScene(sceneAddPeer, storelimit.AddPeer)

	re.Equal(sceneAddPeer, limiter.scene[storelimit.AddPeer])

	sceneRemovePeer := &storelimit.Scene{Idle: 5, Low: 4, Normal: 3, High: 2}
	limiter.ReplaceStoreLimitScene(sceneRemovePeer, storelimit.RemovePeer)
}
