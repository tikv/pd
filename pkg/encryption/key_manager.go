// Copyright 2020 TiKV Project Authors.
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

package encryption

import (
	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

// KeyManager maintains the list to encryption keys. It handles encryption key generation and
// rotation, persisting and loading encryption keys.
type KeyManager interface {
	GetCurrentKey() (keyID uint64, key *encryptionpb.DataKey, err error)
	GetKey(keyID uint64) (key *encryptionpb.DataKey, err error)
}
