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

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type storeHeartbeatClient struct {
	pdpb.PDClient
	resp *pdpb.StoreHeartbeatResponse
	err  error
}

func (c *storeHeartbeatClient) StoreHeartbeat(
	context.Context, *pdpb.StoreHeartbeatRequest, ...grpc.CallOption,
) (*pdpb.StoreHeartbeatResponse, error) {
	return c.resp, c.err
}

func TestStoreHeartbeatFailuresAreRecorded(t *testing.T) {
	testCases := []struct {
		name      string
		client    pdpb.PDClient
		wantCount uint64
		wantError string
	}{
		{
			name: "success",
			client: &storeHeartbeatClient{
				resp: &pdpb.StoreHeartbeatResponse{},
			},
		},
		{
			name: "transport error",
			client: &storeHeartbeatClient{
				err: errors.New("transport failure"),
			},
			wantCount: 1,
			wantError: "transport failure",
		},
		{
			name: "response error",
			client: &storeHeartbeatClient{
				resp: &pdpb.StoreHeartbeatResponse{
					Header: &pdpb.ResponseHeader{
						Error: &pdpb.Error{Type: pdpb.ErrorType_UNKNOWN, Message: "store rejected"},
					},
				},
			},
			wantCount: 1,
			wantError: "store rejected",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stores := newStores(1)
			stores.stat[1].Store(&pdpb.StoreStats{StoreId: 1})
			stores.heartbeat(context.Background(), tc.client, 1)
			count, storeID, err := stores.takeStoreHeartbeatFailures()
			require.Equal(t, tc.wantCount, count)
			if tc.wantError != "" {
				require.Equal(t, uint64(1), storeID)
				require.Contains(t, err, tc.wantError)
			}
		})
	}
}
