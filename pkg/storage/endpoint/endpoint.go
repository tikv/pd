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

package endpoint

import (
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/storage/kv"
)

// StorageEndpoint is the base underlying storage endpoint for all other upper
// specific storage backends. It should define some common storage interfaces and operations,
// which provides the default implementations for all kinds of storages.
type StorageEndpoint struct {
	kv.Base
	encryptionKeyManager *encryption.Manager
}

// NewStorageEndpoint creates a new base storage endpoint with the given KV and encryption key manager.
// It should be embedded inside a storage backend.
func NewStorageEndpoint(
	kvBase kv.Base,
	encryptionKeyManager *encryption.Manager,
) *StorageEndpoint {
	return &StorageEndpoint{
		kvBase,
		encryptionKeyManager,
	}
}
