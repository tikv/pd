// Copyright 2021 TiKV Project Authors.
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

package base

import (
	"github.com/tikv/pd/server/encryptionkm"
	"github.com/tikv/pd/server/kv"
)

// Storage is the underlying storage base for all other specific storages.
// It should define some common storage interfaces and operations.
type Storage struct {
	kv.Base
	encryptionKeyManager *encryptionkm.KeyManager
}

// NewStorage creates a new base Storage with the given KV and encryption key manager.
func NewStorage(
	kvBase kv.Base,
	encryptionKeyManager *encryptionkm.KeyManager,
) *Storage {
	return &Storage{
		kvBase,
		encryptionKeyManager,
	}
}
