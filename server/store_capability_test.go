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

package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
)

func TestLogStoreBootstrapRequiresFullStore(t *testing.T) {
	req := &pdpb.BootstrapRequest{
		Store:  &metapb.Store{Id: 1, Labels: []*metapb.StoreLabel{{Key: core.StoreCapabilityLabel, Value: core.FullStoreCapability}}},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1}}},
	}
	require.NoError(t, checkBootstrapRequest(req))
	req.Store.Labels[0].Value = core.LogOnlyStoreCapability
	require.ErrorContains(t, checkBootstrapRequest(req), "cannot bootstrap")
	req.Store.Labels[0].Value = "log-only-v2"
	require.ErrorContains(t, checkBootstrapRequest(req), "unsupported")
}
