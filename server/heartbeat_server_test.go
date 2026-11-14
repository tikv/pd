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
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/errs"
)

type heartbeatTestStream struct {
	pdpb.PD_RegionHeartbeatServer
	send func(*pdpb.RegionHeartbeatResponse) error
	recv func() (*pdpb.RegionHeartbeatRequest, error)
}

func (s *heartbeatTestStream) Send(resp *pdpb.RegionHeartbeatResponse) error {
	return s.send(resp)
}

func (s *heartbeatTestStream) Recv() (*pdpb.RegionHeartbeatRequest, error) {
	return s.recv()
}

func waitHeartbeatSend(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(heartbeatSendTimeout + time.Second):
		t.Fatal("heartbeat send did not finish")
		return nil
	}
}

func TestHeartbeatServerSerializesSend(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		sendErr error
	}{
		{name: "success"},
		{name: "transport error", sendErr: errors.New("transport failed")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			entered := make(chan struct{}, 2)
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			var calls atomic.Int32
			sender := &heartbeatServer{stream: &heartbeatTestStream{
				send: func(*pdpb.RegionHeartbeatResponse) error {
					calls.Add(1)
					entered <- struct{}{}
					<-release
					return testCase.sendErr
				},
			}}
			first, second := make(chan error, 1), make(chan error, 1)
			go func() { first <- sender.Send(&pdpb.RegionHeartbeatResponse{}) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("first heartbeat send did not start")
			}
			go func() { second <- sender.Send(&pdpb.RegionHeartbeatResponse{}) }()
			// Hold the first transport write while the other producer runs.
			select {
			case <-entered:
			case <-time.After(100 * time.Millisecond):
			}
			callsBeforeRelease := calls.Load()
			unblock()
			firstErr, secondErr := waitHeartbeatSend(t, first), waitHeartbeatSend(t, second)
			re.EqualValues(1, callsBeforeRelease, "concurrent producers must share one transport writer")
			if testCase.sendErr == nil {
				re.NoError(firstErr)
				re.NoError(secondErr)
				re.EqualValues(2, calls.Load())
			} else {
				re.ErrorIs(firstErr, testCase.sendErr)
				re.ErrorIs(secondErr, io.EOF)
				re.EqualValues(1, calls.Load(), "a waiting sender must observe the transport error before sending")
			}
		})
	}
}

func TestHeartbeatServerSendTimeout(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered, exited := make(chan struct{}, 2), make(chan struct{}, 2)
	var calls atomic.Int32
	sender := &heartbeatServer{stream: &heartbeatTestStream{
		send: func(*pdpb.RegionHeartbeatResponse) error {
			calls.Add(1)
			entered <- struct{}{}
			<-ctx.Done()
			exited <- struct{}{}
			return ctx.Err()
		},
	}}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- sender.Send(&pdpb.RegionHeartbeatResponse{}) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first heartbeat send did not start")
	}
	go func() { second <- sender.Send(&pdpb.RegionHeartbeatResponse{}) }()
	re.ErrorIs(waitHeartbeatSend(t, first), errs.ErrSendHeartbeatTimeout)
	re.ErrorIs(waitHeartbeatSend(t, second), errs.ErrSendHeartbeatTimeout)
	// The first transport write is still blocked after both callers time out.
	// The second worker must keep waiting for it rather than entering Send.
	re.EqualValues(1, calls.Load())
	cancel()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("transport send did not exit after cancellation")
	}
	re.Never(func() bool { return calls.Load() > 1 }, 100*time.Millisecond, time.Millisecond,
		"a timed-out waiter must not send after acquiring the lock")
}

func TestHeartbeatServerRecvDuringSend(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{}, 1)
	want := &pdpb.RegionHeartbeatRequest{Region: &metapb.Region{Id: 1}}
	sender := &heartbeatServer{stream: &heartbeatTestStream{
		send: func(*pdpb.RegionHeartbeatResponse) error {
			entered <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		},
		recv: func() (*pdpb.RegionHeartbeatRequest, error) { return want, nil },
	}}
	sent := make(chan error, 1)
	go func() { sent <- sender.Send(&pdpb.RegionHeartbeatResponse{}) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("heartbeat send did not start")
	}
	got, err := sender.Recv()
	re.NoError(err)
	re.Same(want, got)
	cancel()
	re.ErrorIs(waitHeartbeatSend(t, sent), context.Canceled)
}
