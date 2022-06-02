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
	"crypto/aes"
	"crypto/cipher"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
)

type testKeyManager struct {
	Keys              *encryptionpb.KeyDictionary
	EncryptionEnabled bool
}

func newTestKeyManager() *testKeyManager {
	return &testKeyManager{
		EncryptionEnabled: true,
		Keys: &encryptionpb.KeyDictionary{
			CurrentKeyId: 2,
			Keys: map[uint64]*encryptionpb.DataKey{
				1: {
					Key:          []byte("\x05\x4f\xc7\x9b\xa3\xa4\xc8\x99\x37\x44\x55\x21\x6e\xd4\x8d\x5d"),
					Method:       encryptionpb.EncryptionMethod_AES128_CTR,
					CreationTime: 1599608041,
					WasExposed:   false,
				},
				2: {
					Key:          []byte("\x6c\xe0\xc1\xfc\xb2\x0d\x38\x18\x50\xcb\xe4\x21\x33\xda\x0d\xb0"),
					Method:       encryptionpb.EncryptionMethod_AES128_CTR,
					CreationTime: 1599608042,
					WasExposed:   false,
				},
			},
		},
	}
}

func (m *testKeyManager) GetCurrentKey() (uint64, *encryptionpb.DataKey, error) {
	if !m.EncryptionEnabled {
		return 0, nil, nil
	}
	currentKeyID := m.Keys.CurrentKeyId
	return currentKeyID, m.Keys.Keys[currentKeyID], nil
}

func (m *testKeyManager) GetKey(keyID uint64) (*encryptionpb.DataKey, error) {
	key, ok := m.Keys.Keys[keyID]
	if !ok {
		return nil, errors.New("missing key")
	}
	return key, nil
}

func TestNilRegion(t *testing.T) {
	re := require.New(t)
	m := newTestKeyManager()
	region, err := EncryptRegion(nil, m)
	re.Error(err)
	re.Nil(region)
	err = DecryptRegion(nil, m)
	re.Error(err)
}

func TestEncryptRegionWithoutKeyManager(t *testing.T) {
	re := require.New(t)
	region := &metapb.Region{
		Id:             10,
		StartKey:       []byte("abc"),
		EndKey:         []byte("xyz"),
		EncryptionMeta: nil,
	}
	region, err := EncryptRegion(region, nil)
	re.NoError(err)
	// check the region isn't changed
	re.Equal("abc", string(region.StartKey))
	re.Equal("xyz", string(region.EndKey))
	re.Nil(region.EncryptionMeta)
}

func TestEncryptRegionWhileEncryptionDisabled(t *testing.T) {
	re := require.New(t)
	region := &metapb.Region{
		Id:             10,
		StartKey:       []byte("abc"),
		EndKey:         []byte("xyz"),
		EncryptionMeta: nil,
	}
	m := newTestKeyManager()
	m.EncryptionEnabled = false
	region, err := EncryptRegion(region, m)
	re.NoError(err)
	// check the region isn't changed
	re.Equal("abc", string(region.StartKey))
	re.Equal("xyz", string(region.EndKey))
	re.Nil(region.EncryptionMeta)
}

func TestEncryptRegion(t *testing.T) {
	re := require.New(t)
	startKey := []byte("abc")
	endKey := []byte("xyz")
	region := &metapb.Region{
		Id:             10,
		StartKey:       make([]byte, len(startKey)),
		EndKey:         make([]byte, len(endKey)),
		EncryptionMeta: nil,
	}
	copy(region.StartKey, startKey)
	copy(region.EndKey, endKey)
	m := newTestKeyManager()
	outRegion, err := EncryptRegion(region, m)
	re.NoError(err)
	re.NotEqual(region, outRegion)
	// check region is encrypted
	re.NotNil(outRegion.EncryptionMeta)
	re.Equal(uint64(2), outRegion.EncryptionMeta.KeyId)
	re.Len(outRegion.EncryptionMeta.Iv, ivLengthCTR)
	// Check encrypted content
	_, currentKey, err := m.GetCurrentKey()
	re.NoError(err)
	block, err := aes.NewCipher(currentKey.Key)
	re.NoError(err)
	stream := cipher.NewCTR(block, outRegion.EncryptionMeta.Iv)
	ciphertextStartKey := make([]byte, len(startKey))
	stream.XORKeyStream(ciphertextStartKey, startKey)
	re.Equal(string(ciphertextStartKey), string(outRegion.StartKey))
	ciphertextEndKey := make([]byte, len(endKey))
	stream.XORKeyStream(ciphertextEndKey, endKey)
	re.Equal(string(ciphertextEndKey), string(outRegion.EndKey))
}

func TestDecryptRegionNotEncrypted(t *testing.T) {
	re := require.New(t)
	region := &metapb.Region{
		Id:             10,
		StartKey:       []byte("abc"),
		EndKey:         []byte("xyz"),
		EncryptionMeta: nil,
	}
	m := newTestKeyManager()
	err := DecryptRegion(region, m)
	re.NoError(err)
	// check the region isn't changed
	re.Equal("abc", string(region.StartKey))
	re.Equal("xyz", string(region.EndKey))
	re.Nil(region.EncryptionMeta)
}

func TestDecryptRegionWithoutKeyManager(t *testing.T) {
	re := require.New(t)
	region := &metapb.Region{
		Id:       10,
		StartKey: []byte("abc"),
		EndKey:   []byte("xyz"),
		EncryptionMeta: &encryptionpb.EncryptionMeta{
			KeyId: 2,
			Iv:    []byte("\x03\xcc\x30\xee\xef\x9a\x19\x79\x71\x38\xbb\x6a\xe5\xee\x31\x86"),
		},
	}
	err := DecryptRegion(region, nil)
	re.Error(err)
}

func TestDecryptRegionWhileKeyMissing(t *testing.T) {
	re := require.New(t)
	keyID := uint64(3)
	m := newTestKeyManager()
	_, err := m.GetKey(3)
	re.Error(err)

	region := &metapb.Region{
		Id:       10,
		StartKey: []byte("abc"),
		EndKey:   []byte("xyz"),
		EncryptionMeta: &encryptionpb.EncryptionMeta{
			KeyId: keyID,
			Iv:    []byte("\x03\xcc\x30\xee\xef\x9a\x19\x79\x71\x38\xbb\x6a\xe5\xee\x31\x86"),
		},
	}
	err = DecryptRegion(region, m)
	re.Error(err)
}

func TestDecryptRegion(t *testing.T) {
	re := require.New(t)
	keyID := uint64(1)
	startKey := []byte("abc")
	endKey := []byte("xyz")
	iv := []byte("\x03\xcc\x30\xee\xef\x9a\x19\x79\x71\x38\xbb\x6a\xe5\xee\x31\x86")
	region := &metapb.Region{
		Id:       10,
		StartKey: make([]byte, len(startKey)),
		EndKey:   make([]byte, len(endKey)),
		EncryptionMeta: &encryptionpb.EncryptionMeta{
			KeyId: keyID,
			Iv:    make([]byte, len(iv)),
		},
	}
	copy(region.StartKey, startKey)
	copy(region.EndKey, endKey)
	copy(region.EncryptionMeta.Iv, iv)
	m := newTestKeyManager()
	err := DecryptRegion(region, m)
	re.NoError(err)
	// check region is decrypted
	re.Nil(region.EncryptionMeta)
	// Check decrypted content
	key, err := m.GetKey(keyID)
	re.NoError(err)
	block, err := aes.NewCipher(key.Key)
	re.NoError(err)
	stream := cipher.NewCTR(block, iv)
	plaintextStartKey := make([]byte, len(startKey))
	stream.XORKeyStream(plaintextStartKey, startKey)
	re.Equal(string(plaintextStartKey), string(region.StartKey))
	plaintextEndKey := make([]byte, len(endKey))
	stream.XORKeyStream(plaintextEndKey, endKey)
	re.Equal(string(plaintextEndKey), string(region.EndKey))
}
