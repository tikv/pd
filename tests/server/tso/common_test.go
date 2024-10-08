// Copyright 2021 TiKV Project Authors.
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

//go:build tso_full_test || tso_consistency_test || tso_function_test
// +build tso_full_test tso_consistency_test tso_function_test

package tso_test

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/testutil"
	"go.uber.org/goleak"
)

const (
	tsoRequestConcurrencyNumber = 3
	tsoRequestRound             = 30
	tsoCount                    = 10
)

func checkAndReturnTimestampResponse(re *require.Assertions, req *pdpb.TsoRequest, resp *pdpb.TsoResponse) *pdpb.Timestamp {
	re.Equal(req.GetCount(), resp.GetCount())
	timestamp := resp.GetTimestamp()
	re.Positive(timestamp.GetPhysical())
	re.GreaterOrEqual(uint32(timestamp.GetLogical())>>timestamp.GetSuffixBits(), req.GetCount())
	return timestamp
}

func testGetTimestamp(re *require.Assertions, ctx context.Context, pdCli pdpb.PDClient, req *pdpb.TsoRequest) *pdpb.Timestamp {
	tsoClient, err := pdCli.Tso(ctx)
	re.NoError(err)
	defer tsoClient.CloseSend()
	re.NoError(tsoClient.Send(req))
	resp, err := tsoClient.Recv()
	re.NoError(err)
	return checkAndReturnTimestampResponse(re, req, resp)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}
