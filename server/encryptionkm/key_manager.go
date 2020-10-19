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
// See the License for the specific language governing permissions and
// limitations under the License.

package encryptionkm

import (
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	lib "github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/server/election"
	"github.com/tikv/pd/server/kv"
)

// KeyManager maintains the list to encryption keys. It handles encryption key generation and
// rotation, persisting and loading encryption keys.
type KeyManager struct{}

// NewKeyManager creates a new key manager.
func NewKeyManager(kv kv.Base, config *lib.Config) (*KeyManager, error) {
	// TODO: Implement
	return &KeyManager{}, nil
}

// GetCurrentKey get the current encryption key. The key is nil if encryption is not enabled.
func (m *KeyManager) GetCurrentKey() (keyID uint64, key *encryptionpb.DataKey, err error) {
	// TODO: Implement
	return 0, nil, nil
}

// GetKey get the encryption key with the specific key id.
func (m *KeyManager) GetKey(keyID uint64) (key *encryptionpb.DataKey, err error) {
	// TODO: Implement
	return nil, nil
}

// SetLeadership sets the PD leadership of the current node. PD leader is responsible to update
// encryption keys, e.g. key rotation.
func (m *KeyManager) SetLeadership(leadership *election.Leadership) {
	// TODO: Implement
}

// Close close the key manager on PD server shutdown
func (m *KeyManager) Close() {
	// TODO: Implement
}
