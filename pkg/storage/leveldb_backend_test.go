// Copyright 2024 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/encryption"
)

type item struct {
	key   string
	value string
}

var _ levelDBKV = (*item)(nil)

func (i *item) Key() string {
	return i.key
}

func (i *item) Value() ([]byte, error) {
	return []byte(i.value), nil
}

func (i *item) Encrypt(*encryption.Manager) (levelDBKV, error) {
	return i, nil
}

func TestLevelDBBackend(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backend, err := newLevelDBBackend[*item](ctx, t.TempDir(), nil)
	re.NoError(err)
	re.NotNil(backend)
	itemKV := &item{key: "k1", value: "v1"}
	// Save without flush.
	err = backend.SaveKV(itemKV)
	re.NoError(err)
	value, err := backend.Load(itemKV.key)
	re.NoError(err)
	re.Empty(value)
	// Flush and load.
	err = backend.Flush()
	re.NoError(err)
	value, err = backend.Load(itemKV.key)
	re.NoError(err)
	re.Equal(itemKV.value, value)
	// Delete and load.
	err = backend.DeleteKV(itemKV)
	re.NoError(err)
	value, err = backend.Load(itemKV.key)
	re.NoError(err)
	re.Empty(value)
	// Save twice without flush.
	err = backend.SaveKV(itemKV)
	re.NoError(err)
	value, err = backend.Load(itemKV.key)
	re.NoError(err)
	re.Empty(value)
	itemKV.value = "v2"
	err = backend.SaveKV(itemKV)
	re.NoError(err)
	value, err = backend.Load(itemKV.key)
	re.NoError(err)
	re.Empty(value)
	// Delete before flush.
	err = backend.DeleteKV(itemKV)
	re.NoError(err)
	value, err = backend.Load(itemKV.key)
	re.NoError(err)
	re.Empty(value)
	// Flush and load.
	err = backend.Flush()
	re.NoError(err)
	value, err = backend.Load(itemKV.key)
	re.NoError(err)
	re.Equal(itemKV.value, value)
	// Delete and load.
	err = backend.DeleteKV(itemKV)
	re.NoError(err)
	value, err = backend.Load(itemKV.key)
	re.NoError(err)
	re.Empty(value)
	// Close the backend.
	err = backend.Close()
	re.NoError(err)
}
