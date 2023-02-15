// Copyright 2023 TiKV Project Authors.
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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/tsopb"
)

// TSOClient manages resource group info and token request.
type TSOClient interface {
	// GetTSWithinKeyspace gets a timestamp within the given keyspace from the TSO service
	GetTSWithinKeyspace(ctx context.Context, keyspaceID uint32) (int64, int64, error)
}

// GetTSWithinKeyspace gets a timestamp within the given keyspace from the TSO service
// TODO: Refactor and share the TSO streaming framework in the PD client. The implementation
// here is in a basic manner and only for testing and integration purpose -- no batching,
// no async, no pooling, no forwarding, no retry and no deliberate error handling.
func (c *client) GetTSWithinKeyspace(ctx context.Context, keyspaceID uint32) (physical int64, logical int64, err error) {
	tsoClient, err := c.getTSOClient(keyspaceID)
	if err != nil {
		return 0, 0, err
	}
	done := make(chan struct{})
	cctx, cancel := context.WithCancel(ctx)
	go checkStream(cctx, cancel, done)
	stream, err := tsoClient.Tso(cctx)
	done <- struct{}{}
	if err != nil {
		return 0, 0, err
	}
	defer cancel()

	start := time.Now()
	count := uint32(1)
	req := &tsopb.TsoRequest{
		Header:     c.getTSORequestHeader(keyspaceID),
		Count:      count,
		DcLocation: globalDCLocation,
	}

	if err := stream.Send(req); err != nil {
		err = errors.WithStack(err)
		return 0, 0, err
	}
	resp, err := stream.Recv()
	if err != nil {
		err = errors.WithStack(err)
		return 0, 0, err
	}
	requestDurationTSO.Observe(time.Since(start).Seconds())
	tsoBatchSize.Observe(float64(count))

	if resp.GetCount() != count {
		err = errors.WithStack(errTSOLength)
		return 0, 0, err
	}

	// No need to adjust logical part, because the batch size is 1.
	physical, logical, _ = resp.GetTimestamp().GetPhysical(), resp.GetTimestamp().GetLogical(), resp.GetTimestamp().GetSuffixBits()
	return physical, logical, nil
}

func (c *client) getTSORequestHeader(keyspaceID uint32) *tsopb.RequestHeader {
	return &tsopb.RequestHeader{
		ClusterId:  c.clusterID,
		KeyspaceId: keyspaceID,
	}
}

// getTSOClient gets the TSO client of the TSO instance who is in charge of
// the primary replica of the keyspace (group).
// TODO: implement TSO service discovery client side logic
func (c *client) getTSOClient(keyspaceID uint32) (tsopb.TSOClient, error) {
	return nil, nil
}

func checkStream(streamCtx context.Context, cancel context.CancelFunc, done chan struct{}) {
	select {
	case <-done:
		return
	case <-time.After(3 * time.Second):
		cancel()
	case <-streamCtx.Done():
	}
	<-done
}
