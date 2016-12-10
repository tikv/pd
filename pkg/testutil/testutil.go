// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

import (
	"net"

	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/rpcutil"
)

// MustRPCCall fails current test if fails to make a RPC call.
func MustRPCCall(c *check.C, conn net.Conn, request *pdpb.Request) *pdpb.Response {
	resp, err := rpcutil.Call(conn, 0, request)
	c.Assert(err, check.IsNil)
	c.Assert(resp, check.NotNil)
	return resp
}

// MustRPCRequest fails current test if fails to make RPC requests.
func MustRPCRequest(c *check.C, urls string, request *pdpb.Request) *pdpb.Response {
	resp, err := rpcutil.Request(urls, 0, request)
	c.Assert(err, check.IsNil)
	c.Assert(resp, check.NotNil)
	return resp
}
