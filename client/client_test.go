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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pd

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/client/testutil"
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
		{Name: "pd4", ClientUrls: []string{"tmp://pd4"}},
		{Name: "pd1", ClientUrls: []string{"tmp://pd1"}},
		{Name: "pd3", ClientUrls: []string{"tmp://pd3"}},
		{Name: "pd2", ClientUrls: []string{"tmp://pd2"}},
	}
	getURLs := func(ms []*pdpb.Member) (urls []string) {
		for _, m := range ms {
			urls = append(urls, m.GetClientUrls()[0])
		}
		return
	}
	cli := &baseClient{option: newOption()}
	cli.urls.Store([]string{})
	cli.updateURLs(members[1:])
	c.Assert(cli.GetURLs(), DeepEquals, getURLs([]*pdpb.Member{members[1], members[3], members[2]}))
	cli.updateURLs(members[1:])
	c.Assert(cli.GetURLs(), DeepEquals, getURLs([]*pdpb.Member{members[1], members[3], members[2]}))
	cli.updateURLs(members)
	c.Assert(cli.GetURLs(), DeepEquals, getURLs([]*pdpb.Member{members[1], members[3], members[2], members[0]}))
}

const testClientURL = "tmp://test.url:5255"

var _ = Suite(&testClientCtxSuite{})

type testClientCtxSuite struct{}

func (s *testClientCtxSuite) TestClientCtx(c *C) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*3)
	defer cancel()
	_, err := NewClientWithContext(ctx, []string{testClientURL}, SecurityOption{})
	c.Assert(err, NotNil)
	c.Assert(time.Since(start), Less, time.Second*5)
}

func (s *testClientCtxSuite) TestClientWithRetry(c *C) {
	start := time.Now()
	_, err := NewClientWithContext(context.TODO(), []string{testClientURL}, SecurityOption{}, WithMaxErrorRetry(5))
	c.Assert(err, NotNil)
	c.Assert(time.Since(start), Less, time.Second*10)
}

var _ = Suite(&testClientDialOptionSuite{})

type testClientDialOptionSuite struct{}

func (s *testClientDialOptionSuite) TestGRPCDialOption(c *C) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), 500*time.Millisecond)
	defer cancel()
	cli := &baseClient{
		checkLeaderCh:        make(chan struct{}, 1),
		checkTSODispatcherCh: make(chan struct{}, 1),
		ctx:                  ctx,
		cancel:               cancel,
		security:             SecurityOption{},
		option:               newOption(),
	}
	cli.urls.Store([]string{testClientURL})
	cli.option.gRPCDialOptions = []grpc.DialOption{grpc.WithBlock()}
	err := cli.updateMember()
	c.Assert(err, NotNil)
	c.Assert(time.Since(start), Greater, 500*time.Millisecond)
}

var _ = Suite(&testTsoRequestSuite{})

type testTsoRequestSuite struct{}

func (s *testTsoRequestSuite) TestTsoRequestWait(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	req := &tsoRequest{
		done:       make(chan error, 1),
		physical:   0,
		logical:    0,
		requestCtx: context.TODO(),
		clientCtx:  ctx,
	}
	cancel()
	_, _, err := req.Wait()
	c.Assert(errors.Cause(err), Equals, context.Canceled)

	ctx, cancel = context.WithCancel(context.Background())
	req = &tsoRequest{
		done:       make(chan error, 1),
		physical:   0,
		logical:    0,
		requestCtx: ctx,
		clientCtx:  context.TODO(),
	}
	cancel()
	_, _, err = req.Wait()
	c.Assert(errors.Cause(err), Equals, context.Canceled)
}

const (
	testMaxTSOBatchSize      = 20
	testTSORequestInterval   = 200 * time.Millisecond
	testMaxBatchWaitInterval = 900 * time.Millisecond
	testTSORequestCount      = 15 // total 3 seconds
)

func (s *testTsoRequestSuite) TestFetchPendingRequests(c *C) {
	testcases := []struct {
		preSleep            time.Duration
		bestBatchSize       int
		expectedSize        int
		expectedTimeLess    time.Duration // 0 means no requirement
		expectedTimeGreater time.Duration // 0 means no requirement
	}{
		{
			// Turn off the strategy, do not wait.
			preSleep:         300 * time.Millisecond,
			bestBatchSize:    0,
			expectedSize:     2,
			expectedTimeLess: 50 * time.Millisecond,
		},
		{
			// Return after the bestBatchSize is satisfied.
			bestBatchSize:    3,
			expectedSize:     3,
			expectedTimeLess: 500 * time.Millisecond,
		},
		{
			// Return after the maxBatchWaitInterval is satisfied.
			bestBatchSize:       15,
			expectedSize:        5,
			expectedTimeGreater: testMaxBatchWaitInterval - 100*time.Millisecond,
			expectedTimeLess:    testMaxBatchWaitInterval + 100*time.Millisecond,
		},
	}

	for _, testcase := range testcases {
		tbc := newTestTSOBatchController()
		// pre sleep
		if testcase.preSleep > 0 {
			time.Sleep(testcase.preSleep)
		}
		now := time.Now()
		// `bestBatchSize > 0` means enable strategy, set the relevant parameters.
		var maxBatchWaitInterval time.Duration
		if testcase.bestBatchSize > 0 {
			tbc.statStartTime = now
			tbc.countTSOQuery = 0
			tbc.countTSORequest = 0
			tbc.countBatchWaitDuration = 0
			tbc.countNotExtraTSOQuery = 0
			tbc.bestBatchSize = testcase.bestBatchSize
			maxBatchWaitInterval = testMaxBatchWaitInterval
		}
		c.Assert(tbc.fetchPendingRequests(tbc.ctx, maxBatchWaitInterval), IsNil)
		c.Assert(tbc.collectedRequestCount, Equals, testcase.expectedSize)
		duration := time.Since(now)
		if testcase.expectedTimeLess > 0 {
			c.Assert(duration < testcase.expectedTimeLess, IsTrue)
		}
		if testcase.expectedTimeGreater > 0 {
			c.Assert(duration > testcase.expectedTimeGreater, IsTrue)
		}
		tbc.Close()
	}
}

func (s *testTsoRequestSuite) TestAdjustBestBatchSize(c *C) {
	tbc := newTestTSOBatchController()
	defer tbc.Close()
	// Test stop and start of statistics
	tbc.bestBatchSize = 10
	tbc.statStartTime = time.Now()
	tbc.countTSOQuery = 10
	tbc.countTSORequest = 5
	tbc.countBatchWaitDuration = time.Millisecond
	tbc.countNotExtraTSOQuery = 5

	tbc.adjustBestBatchSize(0) // stop
	c.Assert(tbc.statStartTime, Equals, ZeroTime)

	tbc.adjustBestBatchSize(time.Millisecond) // start
	c.Assert(tbc.statStartTime, Not(Equals), ZeroTime)
	c.Assert(tbc.countTSOQuery, Equals, int64(0))
	c.Assert(tbc.countTSORequest, Equals, int64(0))
	c.Assert(tbc.countBatchWaitDuration, Equals, time.Duration(0))
	c.Assert(tbc.countNotExtraTSOQuery, Equals, int64(0))
	c.Assert(tbc.bestBatchSize, Equals, 0)

	// Test strategy
	tbc.bestBatchSize = 100
	testcases := []struct {
		statDuration           time.Duration
		countTSOQuery          int64
		countTSORequest        int64
		countBatchWaitDuration time.Duration
		countNotExtraTSOQuery  int64
		maxBatchWaitInterval   time.Duration
		expectedBestBatchSize  int
	}{
		// The duration is insufficient and no change will be made
		{
			// statDuration < 2.5s
			time.Second,
			10000,
			1000,
			time.Millisecond,
			9000,
			time.Millisecond,
			100,
		},
		// There are too few requests, turn off the batch strategy
		{
			// countTSORequest < 100 (20req/s * 5s)
			5 * time.Second,
			10,
			10,
			0,
			10,
			time.Millisecond,
			0,
		},
		// avgBatchWaitInterval far exceeds maxBatchWaitInterval, turn off the batch strategy
		{
			// avgBatchWaitInterval = (countBatchWaitDuration / countTSORequest), avgBatchWaitInterval > maxBatchWaitInterval
			// 600ms / 200 req = 3ms/req, 3ms/req > 1ms/req
			5 * time.Second,
			400,
			200,
			600 * time.Millisecond,
			200,
			time.Millisecond,
			0,
		},
		// the waiting time is not long enough
		{
			// avgBatchWaitInterval = 300ms / 5000 = 0.06ms
			// avgBatchWaitInterval < maxBatchWaitInterval(1ms) * 10% -> not long enough
			//
			// avgRequestDuration = (totalDuration - countBatchWaitDuration) / countTSORequest
			//                    = (5000ms - 300ms) / 5000 = 0.94ms
			// bestCountTSORequest = totalDuration / (avgRequestDuration + maxBatchWaitInterval)
			//                     = 5000ms / (0.94ms + 1ms) = 2577.3
			// bestBatchSize = countTSOQuery / bestCountTSORequest = 40000 / 2577.3 = 15.52
			// The actual value = bestBatchSize / 110% = 14.1, so the final value is 14.
			5 * time.Second,
			40000,
			5000,
			300 * time.Millisecond,
			30000,
			time.Millisecond,
			14,
		},
		{
			// avgBatchWaitInterval = 500ms / 10000 = 0.05ms
			// avgBatchWaitInterval < maxBatchWaitInterval(1ms) * 10% -> not long enough
			//
			// avgRequestDuration = (totalDuration - countBatchWaitDuration) / countTSORequest
			//                    = (5000ms - 500ms) / 10000 = 0.45ms
			// bestCountTSORequest = totalDuration / (avgRequestDuration + maxBatchWaitInterval)
			//                     = 5000ms / (0.45ms + 1ms) = 3448.3
			// bestBatchSize = countTSOQuery / bestCountTSORequest = 40000 / 3448.3 = 11.6
			// The actual value = bestBatchSize / 110% = 10.54, so the final value is 11.
			5 * time.Second,
			40000,
			10000,
			500 * time.Millisecond,
			30000,
			time.Millisecond,
			11,
		},
		{
			// avgBatchWaitInterval = 800ms / 10000 = 0.08ms
			// avgBatchWaitInterval < maxBatchWaitInterval(1ms) * 10% -> not long enough
			//
			// avgRequestDuration = (totalDuration - countBatchWaitDuration) / countTSORequest
			//                    = (8000ms - 800ms) / 10000 = 0.72ms
			// bestCountTSORequest = totalDuration / (avgRequestDuration + maxBatchWaitInterval)
			//                     = 8000ms / (0.72ms + 1ms) = 4651.2
			// bestBatchSize = countTSOQuery / bestCountTSORequest = 40000 / 4651.2 = 8.6
			// The actual value = bestBatchSize / 110% = 7.8, so the final value is 8.
			8 * time.Second,
			40000,
			10000,
			800 * time.Millisecond,
			30000,
			time.Millisecond,
			8,
		},
		// the waiting time is long enough
		{
			// avgBatchWaitInterval = 600ms / 5000 = 0.12ms
			// avgBatchWaitInterval > maxBatchWaitInterval(1ms) * 10% -> long enough
			//
			// avgExtraBatchSize = (countTSOQuery - countNotExtraTSOQuery) / countTSORequest
			//                   = (40000 - 4000) / 5000 = 7.2
			// avgNotExtraBatchSize = countNotExtraTSOQuery / countTSORequest = 4000 / 5000 = 0.8
			// avgBatchWaitInterval = countBatchWaitDuration / countTSORequest = 600ms / 5000 = 0.12ms
			// bestExtraBatchSize = avgExtraBatchSize / avgBatchWaitInterval * maxBatchWaitInterval
			//                    = 7.2 / 0.12ms * 1ms = 60
			// bestBatchSize = bestExtraBatchSize + avgNotExtraBatchSize / 110% = 0.8 + 60/1.1 = 55.35
			5 * time.Second,
			40000,
			5000,
			600 * time.Millisecond,
			4000,
			time.Millisecond,
			55,
		},
		{
			// avgBatchWaitInterval = 2000ms / 5000 = 0.4ms
			// avgBatchWaitInterval > maxBatchWaitInterval(1ms) * 10% -> long enough
			//
			// avgExtraBatchSize = (countTSOQuery - countNotExtraTSOQuery) / countTSORequest
			//                   = (40000 - 32000) / 5000 = 1.6
			// avgNotExtraBatchSize = countNotExtraTSOQuery / countTSORequest = 32000 / 5000 = 6.4
			// avgBatchWaitInterval = countBatchWaitDuration / countTSORequest = 2000ms / 5000 = 0.4ms
			// bestExtraBatchSize = avgExtraBatchSize / avgBatchWaitInterval * maxBatchWaitInterval
			//                    = 1.6 / 0.4ms * 1ms = 4
			// bestBatchSize = bestExtraBatchSize + avgNotExtraBatchSize / 110% = 6.4 + 4/1.1 = 10.03
			5 * time.Second,
			40000,
			5000,
			2 * time.Second,
			32000,
			time.Millisecond,
			10,
		},
		{
			// avgBatchWaitInterval = 4000ms / 4000 = 1ms
			// avgBatchWaitInterval > maxBatchWaitInterval(1ms) * 10% -> long enough
			//
			// avgExtraBatchSize = (countTSOQuery - countNotExtraTSOQuery) / countTSORequest
			//                   = (40000 - 20000) / 4000 = 5
			// avgNotExtraBatchSize = countNotExtraTSOQuery / countTSORequest = 20000 / 5000 = 5
			// avgBatchWaitInterval = countBatchWaitDuration / countTSORequest = 4000ms / 4000 = 1ms
			// bestExtraBatchSize = avgExtraBatchSize / avgBatchWaitInterval * maxBatchWaitInterval
			//                    = 5 / 1ms * 1ms = 5
			// bestBatchSize = bestExtraBatchSize + avgNotExtraBatchSize / 110% = 5 + 5/1.1 = 9.55
			8 * time.Second,
			40000,
			4000,
			4 * time.Second,
			2000,
			time.Millisecond,
			9,
		},
	}
	for _, testcase := range testcases {
		tbc.statStartTime = time.Now().Add(-testcase.statDuration)
		tbc.countTSOQuery = testcase.countTSOQuery
		tbc.countTSORequest = testcase.countTSORequest
		tbc.countBatchWaitDuration = testcase.countBatchWaitDuration
		tbc.countNotExtraTSOQuery = testcase.countNotExtraTSOQuery
		tbc.adjustBestBatchSize(testcase.maxBatchWaitInterval)
		c.Assert(tbc.bestBatchSize, Equals, testcase.expectedBestBatchSize)
	}
}

type testTsoBatchController struct {
	*tsoBatchController
	ctx             context.Context
	cancel          context.CancelFunc
	requestInterval time.Duration
	requestCount    int
}

func newTestTSOBatchController() *testTsoBatchController {
	tsoRequestCh := make(chan *tsoRequest, testMaxTSOBatchSize*2)
	ctx, cancel := context.WithCancel(context.Background())
	tbc := &testTsoBatchController{
		tsoBatchController: newTSOBatchController(tsoRequestCh, testMaxTSOBatchSize),
		ctx:                ctx,
		cancel:             cancel,
		requestInterval:    testTSORequestInterval,
		requestCount:       testTSORequestCount,
	}
	tbc.statTicker = time.NewTicker(time.Hour) // don't let it trigger
	go tbc.Run()
	return tbc
}

func (tbc *testTsoBatchController) Run() {
	ticker := time.NewTicker(tbc.requestInterval)
	defer ticker.Stop()
	defer close(tbc.tsoRequestCh)
	for i := 0; i < tbc.requestCount; i++ {
		tbc.tsoRequestCh <- &tsoRequest{
			done:       make(chan error, 1),
			physical:   0,
			logical:    0,
			requestCtx: tbc.ctx,
			clientCtx:  context.TODO(),
		}
		select {
		case <-tbc.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (tbc *testTsoBatchController) Close() {
	tbc.cancel()
	tbc.statTicker.Stop()
}
