// Copyright 2024 TiKV Project Authors.
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

package metastorage

import (
	"context"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"

	"github.com/tikv/pd/client/opt"
)

// Client is the interface for meta storage client.
type Client interface {
	// Watch watches on a key or prefix.
	Watch(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (chan []*meta_storagepb.Event, error)
	// Get gets the value for a key.
	Get(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error)
	// Put puts a key-value pair into meta storage.
	Put(ctx context.Context, key []byte, value []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error)
}
