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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/client/errs"
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

type mockTSOStreamImpl struct {
	ctx        context.Context
	requestCh  chan requestMsg
	resultCh   chan resultMsg
	keyspaceID uint32
	errorState error

	autoGenerateResult bool
	// Current progress of generating TSO results
	resGenPhysical, resGenLogical int64
}

func newMockTSOStreamImpl(ctx context.Context, autoGenerateResult bool) *mockTSOStreamImpl {
	return &mockTSOStreamImpl{
		ctx:        ctx,
		requestCh:  make(chan requestMsg, 64),
		resultCh:   make(chan resultMsg, 64),
		keyspaceID: 0,

		autoGenerateResult: autoGenerateResult,
		resGenPhysical:     10000,
		resGenLogical:      0,
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

func (s *mockTSOStreamImpl) Recv() (ret tsoRequestResult, retErr error) {
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

	var res resultMsg
	needGenRes := false
	if s.autoGenerateResult {
		select {
		case res = <-s.resultCh:
		default:
			needGenRes = true
		}
	} else {
		select {
		case res = <-s.resultCh:
		case <-s.ctx.Done():
			s.errorState = s.ctx.Err()
			return tsoRequestResult{}, s.errorState
		}
	}

	if !res.breakStream {
		var req requestMsg
		select {
		case req = <-s.requestCh:
		case <-s.ctx.Done():
			s.errorState = s.ctx.Err()
			return tsoRequestResult{}, s.errorState
		}
		if needGenRes {

			physical := s.resGenPhysical
			logical := s.resGenLogical + req.count
			if logical >= (1 << 18) {
				physical += logical >> 18
				logical &= (1 << 18) - 1
			}

			s.resGenPhysical = physical
			s.resGenLogical = logical

			res = resultMsg{
				r: tsoRequestResult{
					physical:            s.resGenPhysical,
					logical:             s.resGenLogical,
					count:               uint32(req.count),
					suffixBits:          0,
					respKeyspaceGroupID: 0,
				},
			}
		}
	}
	if res.err != nil {
		s.errorState = res.err
	}
	return res.r, res.err
}

func (s *mockTSOStreamImpl) returnResult(physical int64, logical int64, count uint32) {
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
	s.inner = newMockTSOStreamImpl(context.Background(), false)
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

	for i := 0; i < pendingRequests; i++ {
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
	for i := 0; i < count; i++ {
		ch := s.mustProcessRequestWithResultCh(int64(i + 1))
		resultChs = append(resultChs, ch)
	}

	for _, ch := range resultChs {
		s.noResult(ch)
	}

	for i := 0; i < count; i++ {
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

func BenchmarkTSOStreamSendRecv(b *testing.B) {
	streamInner := newMockTSOStreamImpl(context.Background(), true)
	stream := tsoStream{
		serverURL: mockStreamURL,
		stream:    streamInner,
	}
	defer streamInner.stop()

	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _, _ = stream.processRequests(1, 1, 1, globalDCLocation, 1, now)
	}
	b.StopTimer()
}
