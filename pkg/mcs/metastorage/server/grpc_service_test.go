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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

type watchServingServer struct{ bs.Server }

func (*watchServingServer) IsServing() bool { return true }

type metaWatchStream struct {
	grpc.ServerStream
	ctx  context.Context
	send func(*meta_storagepb.WatchResponse) error
}

func (s *metaWatchStream) Context() context.Context { return s.ctx }

func (s *metaWatchStream) Send(response *meta_storagepb.WatchResponse) error {
	return s.send(response)
}

func TestWatchPreservesRevisionAndPreviousValue(t *testing.T) {
	re := require.New(t)
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer cleanup()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	put, err := client.Put(ctx, "watch/key", "previous")
	re.NoError(err)
	deleted, err := client.Delete(ctx, "watch/key")
	re.NoError(err)
	service := &Service{ctx: ctx, manager: &Manager{srv: &watchServingServer{}, client: client}}
	request := &meta_storagepb.WatchRequest{
		Key: []byte("watch/"), RangeEnd: []byte(clientv3.GetPrefixRangeEnd("watch/")), StartRevision: put.Header.Revision, PrevKv: true,
	}
	completed := errors.New("received deletion")
	var events []*meta_storagepb.Event
	err = service.Watch(request, &metaWatchStream{ctx: ctx, send: func(response *meta_storagepb.WatchResponse) error {
		events = append(events, response.Events...)
		if response.Header.Revision == deleted.Header.Revision {
			return completed
		}
		return nil
	}})
	re.ErrorIs(err, completed)
	re.Len(events, 2)
	re.Equal(put.Header.Revision, events[0].Kv.ModRevision)
	re.Equal(deleted.Header.Revision, events[1].Kv.ModRevision)
	re.Equal(meta_storagepb.Event_DELETE, events[1].Type)
	re.Equal([]byte("previous"), events[1].PrevKv.Value)

	_, err = client.Compact(ctx, deleted.Header.Revision)
	re.NoError(err)
	var compacted *meta_storagepb.WatchResponse
	err = service.Watch(request, &metaWatchStream{ctx: ctx, send: func(response *meta_storagepb.WatchResponse) error {
		compacted = response
		return nil
	}})
	re.ErrorIs(err, rpctypes.ErrCompacted)
	re.NotNil(compacted)
	re.Equal(meta_storagepb.ErrorType_DATA_COMPACTED, compacted.Header.Error.Type)
	re.Equal(deleted.Header.Revision, compacted.CompactRevision)
}
