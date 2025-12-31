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
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

func TestEncryptionMethodSupported(t *testing.T) {
	re := require.New(t)
	re.Error(CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_PLAINTEXT))
	re.Error(CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_UNKNOWN))
	re.NoError(CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_AES128_CTR))
	re.NoError(CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_AES192_CTR))
	re.NoError(CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_AES256_CTR))
}

func TestKeyLength(t *testing.T) {
	re := require.New(t)
	_, err := KeyLength(encryptionpb.EncryptionMethod_PLAINTEXT)
	re.Error(err)
	_, err = KeyLength(encryptionpb.EncryptionMethod_UNKNOWN)
	re.Error(err)
	length, err := KeyLength(encryptionpb.EncryptionMethod_AES128_CTR)
	re.NoError(err)
	re.Equal(16, length)
	length, err = KeyLength(encryptionpb.EncryptionMethod_AES192_CTR)
	re.NoError(err)
	re.Equal(24, length)
	length, err = KeyLength(encryptionpb.EncryptionMethod_AES256_CTR)
	re.NoError(err)
	re.Equal(32, length)
}

func TestNewIv(t *testing.T) {
	re := require.New(t)
	ivCtr, err := NewIvCTR()
	re.NoError(err)
	re.Len([]byte(ivCtr), ivLengthCTR)
	ivGcm, err := NewIvGCM()
	re.NoError(err)
	re.Len([]byte(ivGcm), ivLengthGCM)
}

func TestNewDataKey(t *testing.T) {
	re := require.New(t)
	for _, method := range []encryptionpb.EncryptionMethod{
		encryptionpb.EncryptionMethod_AES128_CTR,
		encryptionpb.EncryptionMethod_AES192_CTR,
		encryptionpb.EncryptionMethod_AES256_CTR,
	} {
		_, key, err := NewDataKey(method, uint64(123))
		re.NoError(err)
		length, err := KeyLength(method)
		re.NoError(err)
		re.Len(key.Key, length)
		re.Equal(method, key.Method)
		re.False(key.WasExposed)
		re.Equal(uint64(123), key.CreationTime)
	}
}

func TestAesGcmCrypter(t *testing.T) {
	re := require.New(t)
	key, err := hex.DecodeString("ed568fbd8c8018ed2d042a4e5d38d6341486922d401d2022fb81e47c900d3f07")
	re.NoError(err)
	plaintext, err := hex.DecodeString(
		"5c873a18af5e7c7c368cb2635e5a15c7f87282085f4b991e84b78c5967e946d4")
	re.NoError(err)
	// encrypt
	ivBytes, err := hex.DecodeString("ba432b70336c40c39ba14c1b")
	re.NoError(err)
	iv := IvGCM(ivBytes)
	ciphertext, err := aesGcmEncryptImpl(key, plaintext, iv)
	re.NoError(err)
	re.Len([]byte(iv), ivLengthGCM)
	re.Equal(
		"bbb9b49546350880cf55d4e4eaccc831c506a4aeae7f6cda9c821d4cb8cfc269dcdaecb09592ef25d7a33b40d3f02208",
		hex.EncodeToString(ciphertext),
	)
	// decrypt
	plaintext2, err := AesGcmDecrypt(key, ciphertext, iv)
	re.NoError(err)
	re.True(bytes.Equal(plaintext2, plaintext))
	// Modify ciphertext to test authentication failure. We modify the beginning of the ciphertext,
	// which is the real ciphertext part, not the tag.
	fakeCiphertext := make([]byte, len(ciphertext))
	copy(fakeCiphertext, ciphertext)
	// ignore overflow
	fakeCiphertext[0] = ciphertext[0] + 1
	_, err = AesGcmDecrypt(key, fakeCiphertext, iv)
	re.Error(err)
}
