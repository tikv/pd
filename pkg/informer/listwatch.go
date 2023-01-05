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

// Lister is any object that knows how to perform an initial list.
type Lister interface {
	List(Options) ([]interface{}, uint64, error)
}

// Watcher is any object that knows how to start a watch on a resource.
type Watcher interface {
	Watch(Options) (Interface, error)
}

// ListerWatcher is any object that knows how to perform an initial list and start a watch on a resource.
type ListerWatcher interface {
	Lister
	Watcher
}

// ListFunc knows how to list resources.
type ListFunc func(options Options) ([]interface{}, uint64, error)

// WatchFunc knows how to watch resources
type WatchFunc func(options Options) (Interface, error)

// ListWatch knows how to list and watch a set of resources.
type ListWatch struct {
	ListFunc  ListFunc
	WatchFunc WatchFunc
}

// List a set of apiserver resources
func (lw *ListWatch) List(options Options) ([]interface{}, uint64, error) {
	return lw.ListFunc(options)
}

// Watch a set of apiserver resources
func (lw *ListWatch) Watch(options Options) (Interface, error) {
	return lw.WatchFunc(options)
}

// Interface can be implemented by anything that knows how to watch and report changes.
type Interface interface {
	Stop()
	ResultChan() <-chan []*pdpb.Event
}
