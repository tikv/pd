// Copyright 2019 TiKV Project Authors.
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

package mockhbstream

import (
	"errors"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/opt"
)

// HeartbeatStream is used to mock HeartbeatStream for test use.
type HeartbeatStream struct {
	ch chan *pdpb.RegionHeartbeatResponse
}

// NewHeartbeatStream creates a new HeartbeatStream.
func NewHeartbeatStream() HeartbeatStream {
	return HeartbeatStream{
		ch: make(chan *pdpb.RegionHeartbeatResponse),
	}
}

// Send mocks method.
func (s HeartbeatStream) Send(m *pdpb.RegionHeartbeatResponse) error {
	select {
	case <-time.After(time.Second):
		return errors.New("timeout")
	case s.ch <- m:
	}
	return nil
}

// SendMsg is used to send the message.
func (s HeartbeatStream) SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse) {}

// BindStream mock method.
func (s HeartbeatStream) BindStream(storeID uint64, stream opt.HeartbeatStream) {}

// Recv mocks method.
func (s HeartbeatStream) Recv() *pdpb.RegionHeartbeatResponse {
	select {
	case <-time.After(time.Millisecond * 10):
		return nil
	case res := <-s.ch:
		return res
	}
}
