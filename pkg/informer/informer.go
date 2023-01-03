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

// Informer is used to synchronize the information that we need.
type Informer struct {
	lw        ListerWatcher
	store     Store
	reflector *reflector
}

// NewInformer returns a informer instance.
func NewInformer(lw ListerWatcher, store Store) *Informer {
	return &Informer{lw: lw, store: store}
}

// Run starts to synchronize the info.
func (i *Informer) Run(stopCh <-chan struct{}) error {
	i.reflector = newReflector(i.lw, i.store)
	return i.reflector.run(stopCh)
}
