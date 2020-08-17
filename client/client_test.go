// Copyright 2016 TiKV Project Authors.
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

package pd

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/testutil"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
)

func Test(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = Suite(&testClientSuite{})

type testClientSuite struct{}

func (s *testClientSuite) TestTsLessEqual(c *C) {
	c.Assert(tsLessEqual(9, 9, 9, 9), IsTrue)
	c.Assert(tsLessEqual(8, 9, 9, 8), IsTrue)
	c.Assert(tsLessEqual(9, 8, 8, 9), IsFalse)
	c.Assert(tsLessEqual(9, 8, 9, 6), IsFalse)
	c.Assert(tsLessEqual(9, 6, 9, 8), IsTrue)
}

func (s *testClientSuite) TestUpdateURLs(c *C) {
	members := []*pdpb.Member{
		{Name: "pd4", ClientUrls: []string{"tmp//pd4"}},
		{Name: "pd1", ClientUrls: []string{"tmp//pd1"}},
		{Name: "pd3", ClientUrls: []string{"tmp//pd3"}},
		{Name: "pd2", ClientUrls: []string{"tmp//pd2"}},
	}
	getURLs := func(ms []*pdpb.Member) (urls []string) {
		for _, m := range ms {
			urls = append(urls, m.GetClientUrls()[0])
		}
		return
	}
	cli := &baseClient{}
	cli.updateURLs(members[1:])
	c.Assert(cli.urls, DeepEquals, getURLs([]*pdpb.Member{members[1], members[3], members[2]}))
	cli.updateURLs(members[1:])
	c.Assert(cli.urls, DeepEquals, getURLs([]*pdpb.Member{members[1], members[3], members[2]}))
	cli.updateURLs(members)
	c.Assert(cli.urls, DeepEquals, getURLs([]*pdpb.Member{members[1], members[3], members[2], members[0]}))
}

var _ = Suite(&testClientCtxSuite{})

type testClientCtxSuite struct{}

func (s *testClientCtxSuite) TestClientCtx(c *C) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*3)
	defer cancel()
	_, err := NewClientWithContext(ctx, []string{"localhost:8080"}, SecurityOption{})
	c.Assert(err, NotNil)
	c.Assert(time.Since(start), Less, time.Second*4)
}

var _ = Suite(&testClientDialOptionSuite{})

type testClientDialOptionSuite struct{}

func (s *testClientDialOptionSuite) TestGRPCDialOption(c *C) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), 500*time.Millisecond)
	defer cancel()
	// nolint
	cli := &baseClient{
		urls:            []string{"localhost:8080"},
		checkLeaderCh:   make(chan struct{}, 1),
		ctx:             ctx,
		cancel:          cancel,
		security:        SecurityOption{},
		gRPCDialOptions: []grpc.DialOption{grpc.WithBlock()},
	}
	cli.connMu.clientConns = make(map[string]*grpc.ClientConn)

	err := cli.updateLeader()
	c.Assert(err, NotNil)
	c.Assert(time.Since(start), Greater, 500*time.Millisecond)
}
