// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/metapb"
)

func TestRegionCrypter(t *testing.T) {
	TestingT(t)
}

type testRegionCrypterSuite struct{}

var _ = Suite(&testRegionCrypterSuite{})

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

func (s *testRegionCrypterSuite) TestNilRegion(c *C) {
	m := newTestKeyManager()
	region, err := EncryptRegion(nil, m)
	c.Assert(err, NotNil)
	c.Assert(region, IsNil)
	err = DecryptRegion(nil, m)
	c.Assert(err, NotNil)
}

func (s *testRegionCrypterSuite) TestEncryptRegionWithoutKeyManager(c *C) {
	region := &metapb.Region{
		Id:             10,
		StartKey:       []byte("abc"),
		EndKey:         []byte("xyz"),
		EncryptionMeta: nil,
	}
	region, err := EncryptRegion(region, nil)
	c.Assert(err, IsNil)
	// check the region isn't changed
	c.Assert(string(region.StartKey), Equals, "abc")
	c.Assert(string(region.EndKey), Equals, "xyz")
	c.Assert(region.EncryptionMeta, IsNil)
}

func (s *testRegionCrypterSuite) TestEncryptRegionWhileEncryptionDisabled(c *C) {
	region := &metapb.Region{
		Id:             10,
		StartKey:       []byte("abc"),
		EndKey:         []byte("xyz"),
		EncryptionMeta: nil,
	}
	m := newTestKeyManager()
	m.EncryptionEnabled = false
	region, err := EncryptRegion(region, m)
	c.Assert(err, IsNil)
	// check the region isn't changed
	c.Assert(string(region.StartKey), Equals, "abc")
	c.Assert(string(region.EndKey), Equals, "xyz")
	c.Assert(region.EncryptionMeta, IsNil)
}

func (s *testRegionCrypterSuite) TestEncryptRegion(c *C) {
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
	c.Assert(err, IsNil)
	c.Assert(outRegion, Not(Equals), region)
	// check region is encrypted
	c.Assert(outRegion.EncryptionMeta, Not(IsNil))
	c.Assert(outRegion.EncryptionMeta.KeyId, Equals, uint64(2))
	c.Assert(outRegion.EncryptionMeta.Iv, HasLen, ivLengthCTR)
	// Check encrypted content
	_, currentKey, err := m.GetCurrentKey()
	c.Assert(err, IsNil)
	block, err := aes.NewCipher(currentKey.Key)
	c.Assert(err, IsNil)
	stream := cipher.NewCTR(block, outRegion.EncryptionMeta.Iv)
	ciphertextStartKey := make([]byte, len(startKey))
	stream.XORKeyStream(ciphertextStartKey, startKey)
	c.Assert(string(outRegion.StartKey), Equals, string(ciphertextStartKey))
	ciphertextEndKey := make([]byte, len(endKey))
	stream.XORKeyStream(ciphertextEndKey, endKey)
	c.Assert(string(outRegion.EndKey), Equals, string(ciphertextEndKey))
}

func (s *testRegionCrypterSuite) TestDecryptRegionNotEncrypted(c *C) {
	region := &metapb.Region{
		Id:             10,
		StartKey:       []byte("abc"),
		EndKey:         []byte("xyz"),
		EncryptionMeta: nil,
	}
	m := newTestKeyManager()
	err := DecryptRegion(region, m)
	c.Assert(err, IsNil)
	// check the region isn't changed
	c.Assert(string(region.StartKey), Equals, "abc")
	c.Assert(string(region.EndKey), Equals, "xyz")
	c.Assert(region.EncryptionMeta, IsNil)
}

func (s *testRegionCrypterSuite) TestDecryptRegionWithoutKeyManager(c *C) {
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
	c.Assert(err, Not(IsNil))
}

func (s *testRegionCrypterSuite) TestDecryptRegionWhileKeyMissing(c *C) {
	keyID := uint64(3)
	m := newTestKeyManager()
	_, err := m.GetKey(3)
	c.Assert(err, Not(IsNil))

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
	c.Assert(err, Not(IsNil))
}

func (s *testRegionCrypterSuite) TestDecryptRegion(c *C) {
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
	c.Assert(err, IsNil)
	// check region is decrypted
	c.Assert(region.EncryptionMeta, IsNil)
	// Check decrypted content
	key, err := m.GetKey(keyID)
	c.Assert(err, IsNil)
	block, err := aes.NewCipher(key.Key)
	c.Assert(err, IsNil)
	stream := cipher.NewCTR(block, iv)
	plaintextStartKey := make([]byte, len(startKey))
	stream.XORKeyStream(plaintextStartKey, startKey)
	c.Assert(string(region.StartKey), Equals, string(plaintextStartKey))
	plaintextEndKey := make([]byte, len(endKey))
	stream.XORKeyStream(plaintextEndKey, endKey)
	c.Assert(string(region.EndKey), Equals, string(plaintextEndKey))
}
