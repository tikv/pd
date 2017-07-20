// Copyright 2017 PingCAP, Inc.
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

package server

import (
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"google.golang.org/grpc"
)

var (
	defaultStreamTimeout = time.Second * 5
	errStreamTimeout     = errors.New("receive from grpc stream timeout")
)

type timeoutServerStream struct {
	stream  grpc.ServerStream
	ch      chan proto.Message
	errCh   chan error
	timeout time.Duration
}

func newTimeoutServerStream(stream grpc.ServerStream, newMsg func() proto.Message, timeout time.Duration) *timeoutServerStream {
	ch := make(chan proto.Message, 1)
	errCh := make(chan error, 1)
	go func() {
		for {
			msg := newMsg()
			if err := stream.RecvMsg(msg); err != nil {
				errCh <- errors.Trace(err)
				return
			}
			ch <- msg
		}
	}()
	return &timeoutServerStream{
		stream:  stream,
		ch:      ch,
		errCh:   errCh,
		timeout: timeout,
	}
}

func (s *timeoutServerStream) Recv() (proto.Message, error) {
	select {
	case msg := <-s.ch:
		return msg, nil
	case err := <-s.errCh:
		return nil, err
	case <-time.After(s.timeout):
		return nil, errStreamTimeout
	}
}
