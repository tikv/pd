// Copyright 2022 TiKV Project Authors.
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

package backend

import (
	"github.com/tikv/pd/server/encryptionkm"
	"github.com/tikv/pd/server/kv"
)

// BaseBackend is the base underlying storage backend for all other upper specific storage backends.
// It should define some common storage interfaces and operations, which provides the default
// implementations for all kinds of storages.
type BaseBackend struct {
	kv.Base
	encryptionKeyManager *encryptionkm.KeyManager
}

// NewBaseBackend creates a new base storage backend with the given KV and encryption key manager.
func NewBaseBackend(
	kvBase kv.Base,
	encryptionKeyManager *encryptionkm.KeyManager,
) *BaseBackend {
	return &BaseBackend{
		kvBase,
		encryptionKeyManager,
	}
}

func (bb *BaseBackend) Flush() error { return nil }

func (bb *BaseBackend) Close() error { return nil }
