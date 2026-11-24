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

package endpoint

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestLoadConfigWithRevision(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	store := NewStorageEndpoint(kv.NewEtcdKVBase(client), nil)
	var cfg map[string]string
	revision, err := store.LoadConfigWithRevision(ctx, &cfg)
	re.NoError(err)
	re.Zero(revision)
	for _, invalid := range []string{"", "{"} {
		re.NoError(store.Save(keypath.ConfigPath(), invalid))
		_, err := store.LoadConfigWithRevision(ctx, &cfg)
		re.Error(err)
	}
	put, err := client.Put(ctx, keypath.ConfigPath(), `{"route":"original"}`)
	re.NoError(err)
	// The key's ModRevision, not the cluster's latest revision, fences the snapshot.
	_, err = client.Put(ctx, "unrelated", "value")
	re.NoError(err)
	revision, err = store.LoadConfigWithRevision(ctx, &cfg)
	re.NoError(err)
	re.Equal(put.Header.Revision, revision)
	re.Equal("original", cfg["route"])
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = store.LoadConfigWithRevision(canceled, &cfg)
	re.ErrorIs(err, context.Canceled)
}
