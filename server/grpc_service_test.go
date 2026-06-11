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
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestNewSchedulingAskBatchSplitRequestPreservesReason(t *testing.T) {
	re := require.New(t)
	req := newSchedulingAskBatchSplitRequest(&pdpb.AskBatchSplitRequest{
		Header:     &pdpb.RequestHeader{ClusterId: 1, SenderId: 2},
		Region:     &metapb.Region{Id: 100},
		SplitCount: 3,
		Reason:     pdpb.SplitReason_LOAD,
	})
	re.Equal(uint64(1), req.GetHeader().GetClusterId())
	re.Equal(uint64(2), req.GetHeader().GetSenderId())
	re.Equal(uint64(100), req.GetRegion().GetId())
	re.Equal(uint32(3), req.GetSplitCount())
	re.Equal(pdpb.SplitReason_LOAD, req.GetReason())
}
