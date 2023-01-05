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
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type reflector struct {
	// The destination to sync up with the watch source
	store Store
	// lw is used to perform lists and watches.
	lw ListerWatcher
}

func newReflector(lw ListerWatcher, store Store) *reflector {
	return &reflector{lw: lw, store: store}
}

func (r *reflector) run(stopCh <-chan struct{}) error {
	return r.listAndWatch(stopCh)
}

func (r *reflector) listAndWatch(stopCh <-chan struct{}) error {
	revision, err := r.list()
	if err != nil {
		return err
	}

	for {
		select {
		case <-stopCh:
			return nil
		default:
		}

		w, err := r.lw.Watch(Options{Revision: revision})
		if err != nil {
			return err
		}

		err = r.watchHandler(w, stopCh)
		if err != nil {
			return err
		}
	}
}

func (r *reflector) list() (uint64, error) {
	items, revision, err := r.lw.List(Options{})
	if err != nil {
		return 0, err
	}
	if err := r.syncWith(items); err != nil {
		return 0, err
	}
	return revision, nil
}

// syncWith updates the store's items with the given list.
func (r *reflector) syncWith(items []interface{}) error {
	for _, item := range items {
		err := r.store.Add(item)
		if err != nil {
			return err
		}
	}
	return nil
}

// watchHandler watches w.
func (r *reflector) watchHandler(
	w Interface,
	stopCh <-chan struct{},
) error {
	defer w.Stop()
loop:
	for {
		select {
		case <-stopCh:
			return nil
		case events, ok := <-w.ResultChan():
			if !ok {
				break loop
			}
			for _, event := range events {
				item := event.Item
				switch event.EventType {
				case pdpb.EventType_Added:
					err := r.store.Add(item)
					if err != nil {
						return err
					}
				case pdpb.EventType_Modified:
					err := r.store.Update(item)
					if err != nil {
						return err
					}
				case pdpb.EventType_Deleted:
					err := r.store.Delete(item)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
