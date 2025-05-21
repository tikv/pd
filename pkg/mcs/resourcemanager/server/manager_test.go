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

package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/testutil"
)

type mockConfigProvider struct{ bs.Server }

func (*mockConfigProvider) GetControllerConfig() *ControllerConfig { return &ControllerConfig{} }

func (*mockConfigProvider) AddStartCallback(...func()) {}

func (*mockConfigProvider) AddServiceReadyCallback(...func(context.Context) error) {}

func TestBackgroundMetricsFlush(t *testing.T) {
	re := require.New(t)

	storage := storage.NewStorageWithMemoryBackend()
	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = storage

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	// Add a resource group.
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
	err = m.AddResourceGroup(group)
	re.NoError(err)

	// Send consumption to the dispatcher.
	req := &rmpb.TokenBucketRequest{
		ResourceGroupName: group.GetName(),
		ConsumptionSinceLastRequest: &rmpb.Consumption{
			RRU: 10.0,
			WRU: 20.0,
		},
	}
	m.dispatchConsumption(req)

	// Verify consumption was added to the resource group.
	testutil.Eventually(re, func() bool {
		updatedGroup := m.GetResourceGroup(constant.NullKeyspaceID, req.GetResourceGroupName(), true)
		re.NotNil(updatedGroup)
		return updatedGroup.RUConsumption.RRU == req.ConsumptionSinceLastRequest.RRU &&
			updatedGroup.RUConsumption.WRU == req.ConsumptionSinceLastRequest.WRU
	})
}
