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

package tso_test

import (
	"context"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/testutil"
	"go.uber.org/goleak"
)

// Because we classify TSO integration tests into two parts: tso_function_test and tso_consistency_test,
// the build constraints will make both golangci-lint and gopls have false positives, which is inevitable.
// So `nolint` tag is added here to prevent golangci-lint from reporting the error wrongly.

//nolint:deadcode,unused,varcheck
const (
	tsoRequestConcurrencyNumber = 5
	tsoRequestRound             = 30
	tsoCount                    = 10
)

//nolint:deadcode,unused
func checkAndReturnTimestampResponse(c *C, req *pdpb.TsoRequest, resp *pdpb.TsoResponse) *pdpb.Timestamp {
	c.Assert(resp.GetCount(), Equals, req.GetCount())
	timestamp := resp.GetTimestamp()
	c.Assert(timestamp.GetPhysical(), Greater, int64(0))
	c.Assert(uint32(timestamp.GetLogical())>>timestamp.GetSuffixBits(), GreaterEqual, req.GetCount())
	return timestamp
}

//nolint:deadcode,unused
func testGetTimestamp(c *C, ctx context.Context, pdCli pdpb.PDClient, req *pdpb.TsoRequest) *pdpb.Timestamp {
	tsoClient, err := pdCli.Tso(ctx)
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(err, IsNil)
	return checkAndReturnTimestampResponse(c, req, resp)
}

func Test(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}
