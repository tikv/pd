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
	"bytes"
	"encoding/hex"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

func TestCrypter(t *testing.T) {
	TestingT(t)
}

type testCrypterSuite struct{}

var _ = Suite(&testCrypterSuite{})

func (s *testCrypterSuite) TestEncryptionMethodSupported(c *C) {
	c.Assert(CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_PLAINTEXT), Not(IsNil))
	c.Assert(CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_UNKNOWN), Not(IsNil))
	c.Assert(CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_AES128_CTR), IsNil)
	c.Assert(CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_AES192_CTR), IsNil)
	c.Assert(CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_AES256_CTR), IsNil)
}

func (s *testCrypterSuite) TestKeyLength(c *C) {
	_, err := KeyLength(encryptionpb.EncryptionMethod_PLAINTEXT)
	c.Assert(err, Not(IsNil))
	_, err = KeyLength(encryptionpb.EncryptionMethod_UNKNOWN)
	c.Assert(err, Not(IsNil))
	length, err := KeyLength(encryptionpb.EncryptionMethod_AES128_CTR)
	c.Assert(err, IsNil)
	c.Assert(length, Equals, 16)
	length, err = KeyLength(encryptionpb.EncryptionMethod_AES192_CTR)
	c.Assert(err, IsNil)
	c.Assert(length, Equals, 24)
	length, err = KeyLength(encryptionpb.EncryptionMethod_AES256_CTR)
	c.Assert(err, IsNil)
	c.Assert(length, Equals, 32)
}

func (s *testCrypterSuite) TestNewIv(c *C) {
	ivCtr, err := NewIvCtr()
	c.Assert(err, IsNil)
	c.Assert(len([]byte(ivCtr)), Equals, ivLengthCTR)
	ivGcm, err := NewIvGcm()
	c.Assert(err, IsNil)
	c.Assert(len([]byte(ivGcm)), Equals, ivLengthGCM)
}

func testNewDataKey(c *C, method encryptionpb.EncryptionMethod) {
	_, key, err := NewDataKey(method)
	c.Assert(err, IsNil)
	length, err := KeyLength(method)
	c.Assert(err, IsNil)
	c.Assert(len(key.Key), Equals, length)
	c.Assert(key.Method, Equals, method)
	c.Assert(key.WasExposed, IsFalse)
}

func (s *testCrypterSuite) TestNewDataKey(c *C) {
	testNewDataKey(c, encryptionpb.EncryptionMethod_AES128_CTR)
	testNewDataKey(c, encryptionpb.EncryptionMethod_AES192_CTR)
	testNewDataKey(c, encryptionpb.EncryptionMethod_AES256_CTR)
}

func (s *testCrypterSuite) TestAesGcmCrypter(c *C) {
	key, err := hex.DecodeString("ed568fbd8c8018ed2d042a4e5d38d6341486922d401d2022fb81e47c900d3f07")
	c.Assert(err, IsNil)
	plaintext, err := hex.DecodeString(
		"5c873a18af5e7c7c368cb2635e5a15c7f87282085f4b991e84b78c5967e946d4")
	c.Assert(err, IsNil)
	// encrypt
	ivBytes, err := hex.DecodeString("ba432b70336c40c39ba14c1b")
	c.Assert(err, IsNil)
	iv := IvGcm(ivBytes)
	ciphertext, err := aesGcmEncryptImpl(key, plaintext, iv)
	c.Assert(err, IsNil)
	c.Assert(len([]byte(iv)), Equals, ivLengthGCM)
	c.Assert(
		hex.EncodeToString(ciphertext),
		Equals,
		"bbb9b49546350880cf55d4e4eaccc831c506a4aeae7f6cda9c821d4cb8cfc269dcdaecb09592ef25d7a33b40d3f02208",
	)
	// decrypt
	plaintext2, err := AesGcmDecrypt(key, ciphertext, iv)
	c.Assert(err, IsNil)
	c.Assert(bytes.Equal(plaintext2, plaintext), IsTrue)
	// Modify ciphertext to test authentication failure. We modify the beginning of the ciphertext,
	// which is the real ciphertext part, not the tag.
	fakeCiphertext := make([]byte, len(ciphertext))
	copy(fakeCiphertext, ciphertext)
	// ignore overflow
	fakeCiphertext[0] = ciphertext[0] + 1
	_, err = AesGcmDecrypt(key, fakeCiphertext, iv)
	c.Assert(err, Not(IsNil))
}
