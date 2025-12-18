package servicediscovery

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/client/opt"
)

type fakeMetaStorageClient struct {
	getMu   sync.Mutex
	getResp *meta_storagepb.GetResponse
	getErr  error

	watchMu    sync.Mutex
	watchCh    chan []*meta_storagepb.Event
	watchErr   error
	watchCalls atomic.Int32
}

func (f *fakeMetaStorageClient) Watch(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (chan []*meta_storagepb.Event, error) {
	f.watchMu.Lock()
	defer f.watchMu.Unlock()
	f.watchCalls.Add(1)
	if f.watchErr != nil {
		return nil, f.watchErr
	}
	if f.watchCh == nil {
		f.watchCh = make(chan []*meta_storagepb.Event, 16)
	}
	return f.watchCh, nil
}

func (f *fakeMetaStorageClient) Get(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	f.getMu.Lock()
	defer f.getMu.Unlock()
	return f.getResp, f.getErr
}

func (f *fakeMetaStorageClient) Put(ctx context.Context, key []byte, value []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error) {
	return &meta_storagepb.PutResponse{}, nil
}

func TestResourceManagerDiscoveryInitAndWatch(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	initialURL := "http://127.0.0.1:12345"
	initialParticipant := &resource_manager.Participant{ListenUrls: []string{initialURL}}
	initialValue, err := proto.Marshal(initialParticipant)
	re.NoError(err)

	metaCli := &fakeMetaStorageClient{
		getResp: &meta_storagepb.GetResponse{
			Header: &meta_storagepb.ResponseHeader{Revision: 10},
			Kvs: []*meta_storagepb.KeyValue{{
				Key:   []byte("/ms/1/resource_manager/primary"),
				Value: initialValue,
			}},
			Count: 1,
		},
		watchCh: make(chan []*meta_storagepb.Event, 16),
	}

	var leaderChangedCalls atomic.Int32
	d := NewResourceManagerDiscovery(
		context.Background(),
		1,
		metaCli,
		nil,
		opt.NewOption(),
		func(string) error {
			leaderChangedCalls.Add(1)
			return nil
		},
	)

	re.NoError(d.Init())
	re.NotNil(d.GetConn())
	re.Eventually(func() bool {
		d.mu.RLock()
		defer d.mu.RUnlock()
		return d.serviceURL == initialURL && d.conn != nil
	}, 2*time.Second, 10*time.Millisecond)

	newURL := "http://127.0.0.1:23456"
	newParticipant := &resource_manager.Participant{ListenUrls: []string{newURL}}
	newValue, err := proto.Marshal(newParticipant)
	re.NoError(err)

	metaCli.watchCh <- []*meta_storagepb.Event{{
		Type: meta_storagepb.Event_PUT,
		Kv: &meta_storagepb.KeyValue{
			Key:         []byte("/ms/1/resource_manager/primary"),
			Value:       newValue,
			ModRevision: 11,
		},
	}}

	re.Eventually(func() bool {
		d.mu.RLock()
		defer d.mu.RUnlock()
		return d.serviceURL == newURL
	}, 2*time.Second, 10*time.Millisecond)
	re.Eventually(func() bool {
		return leaderChangedCalls.Load() >= 1
	}, 2*time.Second, 10*time.Millisecond)

	d.Close()
}
