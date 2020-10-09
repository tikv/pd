// Copyright 2020 TiKV Project Authors.
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

package client

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rpc"
	"github.com/tikv/pd/server/core"
)

func TestTable(t *testing.T) {
	TestingT(t)
}

var _ = SerialSuites(&testkvRPCSuite{})

type testkvRPCSuite struct{}

func (s *testkvRPCSuite) TestSendReq(c *C) {
	testcases := []struct {
		injectPath string
		expectErr  error
	}{
		{
			injectPath: "getRegionError",
			expectErr:  errors.New("getRegionError"),
		},
		{
			injectPath: "sendRegionRequestErr",
			expectErr:  errors.New("sendRegionRequestErr"),
		},
	}

	conf := config.DefaultRPC()
	sender := NewRegionRequestSender(&conf)

	for _, testcase := range testcases {
		c.Log(testcase.injectPath)
		c.Assert(failpoint.Enable(fmt.Sprintf("github.com/tikv/pd/pkg/client/%s", testcase.injectPath), "return(true)"), IsNil)
		ctx, cancel := context.WithCancel(context.Background())
		_, err := sender.SendReq(ctx, NewRegionRequest(&rpc.Request{}, &core.RegionInfo{}, &core.StoreInfo{}, time.Second))
		if testcase.expectErr != nil {
			c.Assert(err != nil, Equals, true)
			c.Assert(err.Error(), Equals, testcase.expectErr.Error())
		}
		cancel()
		c.Assert(failpoint.Disable(fmt.Sprintf("github.com/tikv/pd/pkg/client/%s", testcase.injectPath)), IsNil)
	}
}
