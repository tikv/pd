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

package servicediscovery

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/client/clients/metastorage"
	"github.com/tikv/pd/client/opt"
)

type countingMetaStorageClient struct {
	mu        sync.Mutex
	value     []byte
	revision  int64
	callTimes []time.Time
}

func (*countingMetaStorageClient) Watch(context.Context, []byte, ...opt.MetaStorageOption) (chan []*meta_storagepb.Event, error) {
	return nil, errors.New("not implemented")
}

func (c *countingMetaStorageClient) Get(context.Context, []byte, ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.revision++
	c.callTimes = append(c.callTimes, time.Now())
	return &meta_storagepb.GetResponse{
		Header: &meta_storagepb.ResponseHeader{Revision: c.revision},
		Kvs:    []*meta_storagepb.KeyValue{{Value: c.value}},
		Count:  1,
	}, nil
}

func (*countingMetaStorageClient) Put(context.Context, []byte, []byte, ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error) {
	return nil, errors.New("not implemented")
}

func (c *countingMetaStorageClient) snapshotCallTimes() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	callTimes := make([]time.Time, len(c.callTimes))
	copy(callTimes, c.callTimes)
	return callTimes
}

var _ metastorage.Client = (*countingMetaStorageClient)(nil)

func TestResourceManagerServiceURLUpdateBackoff(t *testing.T) {
	oldRetryInterval := serviceURLRetryInterval
	serviceURLRetryInterval = 50 * time.Millisecond
	defer func() {
		serviceURLRetryInterval = oldRetryInterval
	}()

	participant := &rmpb.Participant{ListenUrls: []string{"http://127.0.0.1:1234"}}
	value, err := proto.Marshal(participant)
	require.NoError(t, err)

	metaCli := &countingMetaStorageClient{value: value}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	discovery := NewResourceManagerDiscovery(ctx, 1, metaCli, nil, opt.NewOption(), func(string) error { return nil })
	discovery.serviceURL = participant.ListenUrls[0]

	done := make(chan struct{})
	go func() {
		defer close(done)
		discovery.updateServiceURLLoop(0)
	}()

	discovery.ScheduleUpdateServiceURL()
	require.Eventually(t, func() bool {
		return len(metaCli.snapshotCallTimes()) >= 1
	}, time.Second, 10*time.Millisecond)

	discovery.ScheduleUpdateServiceURL()
	require.Eventually(t, func() bool {
		return len(metaCli.snapshotCallTimes()) >= 2
	}, time.Second, 5*time.Millisecond)

	callTimes := metaCli.snapshotCallTimes()
	require.Len(t, callTimes, 2)
	require.GreaterOrEqual(t, callTimes[1].Sub(callTimes[0]), serviceURLRetryInterval-10*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("resource manager service discovery loop did not exit")
	}
}
