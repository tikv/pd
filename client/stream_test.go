// Copyright 2024 TiKV Project Authors.
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
	"io"
	"math"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/client/errs"
	"go.uber.org/zap/zapcore"
)

const mockStreamURL = "mock:///"

type requestMsg struct {
	clusterID       uint64
	keyspaceGroupID uint32
	count           int64
}

type resultMsg struct {
	r           tsoRequestResult
	err         error
	breakStream bool
}

type resultMode int

const (
	resultModeManual resultMode = iota
	resultModeGenerated
	resultModeGenerateOnSignal
)

type mockTSOStreamImpl struct {
	ctx        context.Context
	requestCh  chan requestMsg
	resultCh   chan resultMsg
	keyspaceID uint32
	errorState error

	resultMode resultMode
	// Current progress of generating TSO results
	resGenPhysical, resGenLogical int64
}

func newMockTSOStreamImpl(ctx context.Context, resultMode resultMode) *mockTSOStreamImpl {
	return &mockTSOStreamImpl{
		ctx:        ctx,
		requestCh:  make(chan requestMsg, 64),
		resultCh:   make(chan resultMsg, 64),
		keyspaceID: 0,

		resultMode:     resultMode,
		resGenPhysical: 10000,
		resGenLogical:  0,
	}
}

func (s *mockTSOStreamImpl) Send(clusterID uint64, _keyspaceID, keyspaceGroupID uint32, _dcLocation string, count int64) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}
	s.requestCh <- requestMsg{
		clusterID:       clusterID,
		keyspaceGroupID: keyspaceGroupID,
		count:           count,
	}
	return nil
}

func (s *mockTSOStreamImpl) Recv() (tsoRequestResult, error) {
	var needGenerateResult, needResultSignal bool
	switch s.resultMode {
	case resultModeManual:
		needResultSignal = true
	case resultModeGenerated:
		needGenerateResult = true
	case resultModeGenerateOnSignal:
		needResultSignal = true
		needGenerateResult = true
	}

	// This stream have ever receive an error, it returns the error forever.
	if s.errorState != nil {
		return tsoRequestResult{}, s.errorState
	}

	select {
	case <-s.ctx.Done():
		s.errorState = s.ctx.Err()
		return tsoRequestResult{}, s.errorState
	default:
	}

	var (
		res    resultMsg
		hasRes bool
		req    requestMsg
		hasReq bool
	)

	// Try to match a pair of request and result from each channel and allowing breaking the stream at any time.
	select {
	case <-s.ctx.Done():
		s.errorState = s.ctx.Err()
		return tsoRequestResult{}, s.errorState
	case req = <-s.requestCh:
		hasReq = true
		select {
		case res = <-s.resultCh:
			hasRes = true
		default:
		}
	case res = <-s.resultCh:
		hasRes = true
		select {
		case req = <-s.requestCh:
			hasReq = true
		default:
		}
	}
	// Either req or res should be ready at this time.

	if hasRes {
		if res.breakStream {
			if res.err == nil {
				panic("breaking mockTSOStreamImpl without error")
			}
			s.errorState = res.err
			return tsoRequestResult{}, s.errorState
		} else if !needResultSignal {
			// Do not allow manually assigning result.
			panic("trying manually specifying result for mockTSOStreamImpl when it's auto-generating mode")
		}
	} else if !needResultSignal {
		// Mark hasRes as true to skip receiving from resultCh. The actual value of the result will be generated later.
		hasRes = true
	}

	if !hasReq {
		// If req is not ready, the res must be ready. So it's certain that it don't need to be canceled by breakStream.
		select {
		case <-s.ctx.Done():
			s.errorState = s.ctx.Err()
			return tsoRequestResult{}, s.errorState
		case req = <-s.requestCh:
			// Skip the assignment to make linter happy.
			// hasReq = true
		}
	} else if !hasRes {
		select {
		case <-s.ctx.Done():
			s.errorState = s.ctx.Err()
			return tsoRequestResult{}, s.errorState
		case res = <-s.resultCh:
			// Skip the assignment to make linter happy.
			// hasRes = true
		}
	}

	if needGenerateResult {
		res = s.autoGenResult(req.count)
	}

	// Both res and req should be ready here.
	if res.err != nil {
		s.errorState = res.err
	}
	return res.r, res.err
}

func (s *mockTSOStreamImpl) autoGenResult(count int64) resultMsg {
	if count >= (1 << 18) {
		panic("requested count too large")
	}
	physical := s.resGenPhysical
	logical := s.resGenLogical + count
	if logical >= (1 << 18) {
		physical += 1
		logical = count
	}

	s.resGenPhysical = physical
	s.resGenLogical = logical

	return resultMsg{
		r: tsoRequestResult{
			physical:            s.resGenPhysical,
			logical:             s.resGenLogical,
			count:               uint32(count),
			suffixBits:          0,
			respKeyspaceGroupID: 0,
		},
	}
}

func (s *mockTSOStreamImpl) returnResult(physical int64, logical int64, count uint32) {
	if s.resultMode != resultModeManual {
		panic("trying to manually specifying tso result on generating mode")
	}
	s.resultCh <- resultMsg{
		r: tsoRequestResult{
			physical:            physical,
			logical:             logical,
			count:               count,
			suffixBits:          0,
			respKeyspaceGroupID: s.keyspaceID,
		},
	}
}

func (s *mockTSOStreamImpl) generateNext() {
	if s.resultMode != resultModeGenerateOnSignal {
		panic("trying to signal generation when the stream is not generate-on-signal mode")
	}
	s.resultCh <- resultMsg{}
}

func (s *mockTSOStreamImpl) returnError(err error) {
	s.resultCh <- resultMsg{
		err: err,
	}
}

func (s *mockTSOStreamImpl) breakStream(err error) {
	s.resultCh <- resultMsg{
		err:         err,
		breakStream: true,
	}
}

func (s *mockTSOStreamImpl) stop() {
	s.breakStream(io.EOF)
}

type callbackInvocation struct {
	result tsoRequestResult
	err    error
}

type testTSOStreamSuite struct {
	suite.Suite
	re *require.Assertions

	inner  *mockTSOStreamImpl
	stream *tsoStream
}

func (s *testTSOStreamSuite) SetupTest() {
	s.re = require.New(s.T())
	s.inner = newMockTSOStreamImpl(context.Background(), resultModeManual)
	s.stream = newTSOStream(context.Background(), mockStreamURL, s.inner)
}

func (s *testTSOStreamSuite) TearDownTest() {
	s.inner.stop()
	s.stream.WaitForClosed()
	s.inner = nil
	s.stream = nil
}

func TestTSOStreamTestSuite(t *testing.T) {
	suite.Run(t, new(testTSOStreamSuite))
}

func (s *testTSOStreamSuite) noResult(ch <-chan callbackInvocation) {
	select {
	case res := <-ch:
		s.re.FailNowf("result received unexpectedly", "received result: %+v", res)
	case <-time.After(time.Millisecond * 20):
	}
}

func (s *testTSOStreamSuite) getResult(ch <-chan callbackInvocation) callbackInvocation {
	select {
	case res := <-ch:
		return res
	case <-time.After(time.Second * 10000):
		s.re.FailNow("result not ready in time")
		panic("result not ready in time")
	}
}

func (s *testTSOStreamSuite) processRequestWithResultCh(count int64) (<-chan callbackInvocation, error) {
	ch := make(chan callbackInvocation, 1)
	err := s.stream.processRequests(1, 2, 3, globalDCLocation, count, time.Now(), func(result tsoRequestResult, reqKeyspaceGroupID uint32, err error) {
		if err == nil {
			s.re.Equal(uint32(3), reqKeyspaceGroupID)
			s.re.Equal(uint32(0), result.suffixBits)
		}
		ch <- callbackInvocation{
			result: result,
			err:    err,
		}
	})
	if err != nil {
		return nil, err
	}
	return ch, nil
}

func (s *testTSOStreamSuite) mustProcessRequestWithResultCh(count int64) <-chan callbackInvocation {
	ch, err := s.processRequestWithResultCh(count)
	s.re.NoError(err)
	return ch
}

func (s *testTSOStreamSuite) TestTSOStreamBasic() {
	ch := s.mustProcessRequestWithResultCh(1)
	s.noResult(ch)
	s.inner.returnResult(10, 1, 1)
	res := s.getResult(ch)

	s.re.NoError(res.err)
	s.re.Equal(int64(10), res.result.physical)
	s.re.Equal(int64(1), res.result.logical)
	s.re.Equal(uint32(1), res.result.count)

	ch = s.mustProcessRequestWithResultCh(2)
	s.noResult(ch)
	s.inner.returnResult(20, 3, 2)
	res = s.getResult(ch)

	s.re.NoError(res.err)
	s.re.Equal(int64(20), res.result.physical)
	s.re.Equal(int64(3), res.result.logical)
	s.re.Equal(uint32(2), res.result.count)

	ch = s.mustProcessRequestWithResultCh(3)
	s.noResult(ch)
	s.inner.returnError(errors.New("mock rpc error"))
	res = s.getResult(ch)
	s.re.Error(res.err)
	s.re.Equal("mock rpc error", res.err.Error())

	// After an error from the (simulated) RPC stream, the tsoStream should be in a broken status and can't accept
	// new request anymore.
	err := s.stream.processRequests(1, 2, 3, globalDCLocation, 1, time.Now(), func(_result tsoRequestResult, _reqKeyspaceGroupID uint32, _err error) {
		panic("unreachable")
	})
	s.re.Error(err)
}

func (s *testTSOStreamSuite) testTSOStreamBrokenImpl(err error, pendingRequests int) {
	var resultCh []<-chan callbackInvocation

	for range pendingRequests {
		ch := s.mustProcessRequestWithResultCh(1)
		resultCh = append(resultCh, ch)
		s.noResult(ch)
	}

	s.inner.breakStream(err)
	closedCh := make(chan struct{})
	go func() {
		s.stream.WaitForClosed()
		closedCh <- struct{}{}
	}()
	select {
	case <-closedCh:
	case <-time.After(time.Second):
		s.re.FailNow("stream receiver loop didn't exit")
	}

	for _, ch := range resultCh {
		res := s.getResult(ch)
		s.re.Error(res.err)
		if err == io.EOF {
			s.re.ErrorIs(res.err, errs.ErrClientTSOStreamClosed)
		} else {
			s.re.ErrorIs(res.err, err)
		}
	}
}

func (s *testTSOStreamSuite) TestTSOStreamBrokenWithEOFNoPendingReq() {
	s.testTSOStreamBrokenImpl(io.EOF, 0)
}

func (s *testTSOStreamSuite) TestTSOStreamCanceledNoPendingReq() {
	s.testTSOStreamBrokenImpl(context.Canceled, 0)
}

func (s *testTSOStreamSuite) TestTSOStreamBrokenWithEOFWithPendingReq() {
	s.testTSOStreamBrokenImpl(io.EOF, 5)
}

func (s *testTSOStreamSuite) TestTSOStreamCanceledWithPendingReq() {
	s.testTSOStreamBrokenImpl(context.Canceled, 5)
}

func (s *testTSOStreamSuite) TestTSOStreamFIFO() {
	var resultChs []<-chan callbackInvocation
	const count = 5
	for i := range count {
		ch := s.mustProcessRequestWithResultCh(int64(i + 1))
		resultChs = append(resultChs, ch)
	}

	for _, ch := range resultChs {
		s.noResult(ch)
	}

	for i := range count {
		s.inner.returnResult(int64((i+1)*10), int64(i), uint32(i+1))
	}

	for i, ch := range resultChs {
		res := s.getResult(ch)
		s.re.NoError(res.err)
		s.re.Equal(int64((i+1)*10), res.result.physical)
		s.re.Equal(int64(i), res.result.logical)
		s.re.Equal(uint32(i+1), res.result.count)
	}
}

func (s *testTSOStreamSuite) TestTSOStreamConcurrentRunning() {
	resultChCh := make(chan (<-chan callbackInvocation), 10000)
	const totalCount = 10000

	// Continuously start requests
	go func() {
		for i := 1; i <= totalCount; i++ {
			// Retry loop
			for {
				ch, err := s.processRequestWithResultCh(int64(i))
				if err != nil {
					// If the capacity of the request queue is exhausted, it returns this error. As a test, we simply
					// spin and retry it until it has enough space, as a coverage of the almost-full case. But note that
					// this should not happen in production, in which case the caller of tsoStream should have its own
					// limit of concurrent RPC requests.
					s.Contains(err.Error(), "unexpected channel full")
					continue
				}

				resultChCh <- ch
				break
			}
		}
	}()

	// Continuously send results
	go func() {
		for i := int64(1); i <= totalCount; i++ {
			s.inner.returnResult(i*10, i%(1<<18), uint32(i))
		}
		s.inner.breakStream(io.EOF)
	}()

	// Check results
	for i := int64(1); i <= totalCount; i++ {
		ch := <-resultChCh
		res := s.getResult(ch)
		s.re.NoError(res.err)
		s.re.Equal(i*10, res.result.physical)
		s.re.Equal(i%(1<<18), res.result.logical)
		s.re.Equal(uint32(i), res.result.count)
	}

	// After handling all these requests, the stream is ended by an EOF error. The next request won't succeed.
	// So, either the `processRequests` function returns an error or the callback is called with an error.
	ch, err := s.processRequestWithResultCh(1)
	if err != nil {
		s.re.ErrorIs(err, errs.ErrClientTSOStreamClosed)
	} else {
		res := s.getResult(ch)
		s.re.Error(res.err)
		s.re.ErrorIs(res.err, errs.ErrClientTSOStreamClosed)
	}
}

func (s *testTSOStreamSuite) TestEstimatedLatency() {
	s.inner.returnResult(100, 0, 1)
	res := s.getResult(s.mustProcessRequestWithResultCh(1))
	s.re.NoError(res.err)
	s.re.Equal(int64(100), res.result.physical)
	s.re.Equal(int64(0), res.result.logical)
	estimation := s.stream.EstimatedRPCLatency().Seconds()
	s.re.Greater(estimation, 0.0)
	s.re.InDelta(0.0, estimation, 0.01)

	// For each began request, record its startTime and send it to the result returning goroutine.
	reqStartTimeCh := make(chan time.Time, maxPendingRequestsInTSOStream)
	// Limit concurrent requests to be less than the capacity of tsoStream.pendingRequests.
	tokenCh := make(chan struct{}, maxPendingRequestsInTSOStream-1)
	for range 40 {
		tokenCh <- struct{}{}
	}
	// Return a result after 50ms delay for each requests
	const delay = time.Millisecond * 50
	// The goroutine to delay and return the result.
	go func() {
		allocated := int64(1)
		for reqStartTime := range reqStartTimeCh {
			now := time.Now()
			elapsed := now.Sub(reqStartTime)
			if elapsed < delay {
				time.Sleep(delay - elapsed)
			}
			s.inner.returnResult(100, allocated, 1)
			allocated++
		}
	}()

	// Limit the test time within 1s
	startTime := time.Now()
	resCh := make(chan (<-chan callbackInvocation), 100)
	// The sending goroutine
	go func() {
		for time.Since(startTime) < time.Second {
			<-tokenCh
			reqStartTimeCh <- time.Now()
			r := s.mustProcessRequestWithResultCh(1)
			resCh <- r
		}
		close(reqStartTimeCh)
		close(resCh)
	}()
	// Check the result
	index := 0
	for r := range resCh {
		// The first is 1
		index++
		res := s.getResult(r)
		tokenCh <- struct{}{}
		s.re.NoError(res.err)
		s.re.Equal(int64(100), res.result.physical)
		s.re.Equal(int64(index), res.result.logical)
	}

	s.re.Greater(s.stream.EstimatedRPCLatency(), time.Duration(int64(0.9*float64(delay))))
	s.re.Less(s.stream.EstimatedRPCLatency(), time.Duration(math.Floor(1.1*float64(delay))))
}

func TestRCFilter(t *testing.T) {
	re := require.New(t)
	// Test basic calculation with frequency 1
	f := newRCFilter(1, 1)
	now := time.Now()
	// The first sample initializes the value.
	re.Equal(10.0, f.update(now, 10))
	now = now.Add(time.Second)
	expectedValue := 10 / (2*math.Pi + 1)
	re.InEpsilon(expectedValue, f.update(now, 0), 1e-8)
	expectedValue = expectedValue*(1/(2*math.Pi))/(1/(2*math.Pi)+2) + 100*2/(1/(2*math.Pi)+2)
	now = now.Add(time.Second * 2)
	re.InEpsilon(expectedValue, f.update(now, 100), 1e-8)

	// Test newSampleWeightUpperBound
	f = newRCFilter(10, 0.5)
	now = time.Now()
	re.Equal(0.0, f.update(now, 0))
	now = now.Add(time.Second)
	re.InEpsilon(1.0, f.update(now, 2), 1e-8)
	now = now.Add(time.Second * 2)
	re.InEpsilon(3.0, f.update(now, 5), 1e-8)

	// Test another cutoff frequency and weight upperbound.
	f = newRCFilter(1/(2*math.Pi), 0.9)
	now = time.Now()
	re.Equal(1.0, f.update(now, 1))
	now = now.Add(time.Second)
	re.InEpsilon(2.0, f.update(now, 3), 1e-8)
	now = now.Add(time.Second * 2)
	re.InEpsilon(6.0, f.update(now, 8), 1e-8)
	now = now.Add(time.Minute)
	re.InEpsilon(15.0, f.update(now, 16), 1e-8)

	// Test with dense samples
	f = newRCFilter(1/(2*math.Pi), 0.9)
	now = time.Now()
	re.Equal(0.0, f.update(now, 0))
	lastOutput := 0.0
	// 10000 even samples in 1 second.
	for range 10000 {
		now = now.Add(time.Microsecond * 100)
		output := f.update(now, 1.0)
		re.Greater(output, lastOutput)
		re.Less(output, 1.0)
		lastOutput = output
	}
	// Regarding the above samples as being close enough to a continuous function, the output after 1 second
	// should be 1 - exp(-RC*t) = 1 - exp(-t). Here RC = 1/(2*pi*cutoff) = 1.
	re.InDelta(0.63, lastOutput, 0.02)
}

func BenchmarkTSOStreamSendRecv(b *testing.B) {
	log.SetLevel(zapcore.FatalLevel)

	streamInner := newMockTSOStreamImpl(context.Background(), resultModeGenerated)
	stream := newTSOStream(context.Background(), mockStreamURL, streamInner)
	defer func() {
		streamInner.stop()
		stream.WaitForClosed()
	}()

	now := time.Now()
	resCh := make(chan tsoRequestResult, 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := stream.processRequests(1, 1, 1, globalDCLocation, 1, now, func(result tsoRequestResult, _ uint32, err error) {
			if err != nil {
				panic(err)
			}
			select {
			case resCh <- result:
			default:
				panic("channel not cleared in the last iteration")
			}
		})
		if err != nil {
			panic(err)
		}
		<-resCh
	}
	b.StopTimer()
}
