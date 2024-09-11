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
