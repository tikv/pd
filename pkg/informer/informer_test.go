// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package informer

import (
	"errors"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/storage/kv"
)

type testLW struct {
	ListFunc  ListFunc
	WatchFunc WatchFunc
}

func (t *testLW) List(options Options) ([]interface{}, uint64, error) {
	return t.ListFunc(options)
}
func (t *testLW) Watch(options Options) (Interface, error) {
	return t.WatchFunc(options)
}

// FakeWatcher lets you test anything that consumes a watch.Interface; threadsafe.
type FakeWatcher struct {
	result  chan *pdpb.Event
	stopped bool
}

func NewFakeWatcher() *FakeWatcher {
	return &FakeWatcher{
		result: make(chan *pdpb.Event),
	}
}

// Stop implements Interface.Stop().
func (f *FakeWatcher) Stop() {
	if !f.stopped {
		close(f.result)
		f.stopped = true
	}
}

func (f *FakeWatcher) ResultChan() <-chan *pdpb.Event {
	return f.result
}

// Add sends an add event.
func (f *FakeWatcher) watch(event *pdpb.Event) {
	f.result <- event
}

func Test(t *testing.T) {
	re := require.New(t)
	fw := NewFakeWatcher()
	store := NewStore(
		kv.NewMemoryKV(),
		func(obj interface{}) (string, error) {
			r, ok := obj.(*pdpb.Item)
			if !ok {
				return "", errors.New("unexpected type")
			}
			return string(r.GetRegion().GetStartKey()), nil
		},
		func(obj interface{}) (string, error) {
			r, ok := obj.(*pdpb.Item)
			if !ok {
				return "", errors.New("unexpected type")
			}
			b, err := proto.Marshal(r.GetRegion())

			return string(b), err
		},
		func(value string) (interface{}, error) {
			r := &metapb.Region{}
			err := proto.Unmarshal([]byte(value), r)
			return r, err
		},
	)
	informer := NewInformer(
		&testLW{
			WatchFunc: func(options Options) (Interface, error) {
				return fw, nil
			},
			ListFunc: func(options Options) ([]interface{}, uint64, error) {
				var l []interface{}
				l = append(l, &pdpb.Item{Item: &pdpb.Item_Region{Region: &metapb.Region{Id: 1, StartKey: []byte("a"), EndKey: []byte("b")}}})
				return l, 1, nil
			},
		},
		store,
	)
	stopCh := make(chan struct{})
	go informer.Run(stopCh)
	fw.watch(&pdpb.Event{EventType: pdpb.EventType_Added, Item: &pdpb.Item{Item: &pdpb.Item_Region{Region: &metapb.Region{Id: 2, StartKey: []byte("b"), EndKey: []byte("c")}}}})
	close(stopCh)
	_, ok := <-fw.ResultChan()
	re.False(ok)

	lister := NewGenericLister(store)
	r, err := lister.Get("a")
	re.NoError(err)
	re.Equal(&metapb.Region{Id: 1, StartKey: []byte("a"), EndKey: []byte("b")}, r)
	r, err = lister.Get("b")
	re.NoError(err)
	re.Equal(&metapb.Region{Id: 2, StartKey: []byte("b"), EndKey: []byte("c")}, r)
	rs, err := lister.List("a", "z", 2)
	re.NoError(err)
	re.Equal(&metapb.Region{Id: 1, StartKey: []byte("a"), EndKey: []byte("b")}, rs[0])
	re.Equal(&metapb.Region{Id: 2, StartKey: []byte("b"), EndKey: []byte("c")}, rs[1])
}
