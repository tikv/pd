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

	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/stretchr/testify/require"
)

func TestEncryptionMethodSupported(t *testing.T) {
	require.NotNil(t, CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_PLAINTEXT))
	require.NotNil(t, CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_UNKNOWN))
	require.Nil(t, CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_AES128_CTR))
	require.Nil(t, CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_AES192_CTR))
	require.Nil(t, CheckEncryptionMethodSupported(encryptionpb.EncryptionMethod_AES256_CTR))
}

func TestKeyLength(t *testing.T) {
	_, err := KeyLength(encryptionpb.EncryptionMethod_PLAINTEXT)
	require.NotNil(t, err)
	_, err = KeyLength(encryptionpb.EncryptionMethod_UNKNOWN)
	require.NotNil(t, err)
	length, err := KeyLength(encryptionpb.EncryptionMethod_AES128_CTR)
	require.NoError(t, err)
	require.Equal(t, 16, length)
	length, err = KeyLength(encryptionpb.EncryptionMethod_AES192_CTR)
	require.NoError(t, err)
	require.Equal(t, 24, length)
	length, err = KeyLength(encryptionpb.EncryptionMethod_AES256_CTR)
	require.NoError(t, err)
	require.Equal(t, 32, length)
}

func TestNewIv(t *testing.T) {
	ivCtr, err := NewIvCTR()
	require.NoError(t, err)
	require.Len(t, []byte(ivCtr), ivLengthCTR)
	ivGcm, err := NewIvGCM()
	require.NoError(t, err)
	require.Len(t, []byte(ivGcm), ivLengthGCM)
}

func testNewDataKey(t *testing.T, method encryptionpb.EncryptionMethod) {
	_, key, err := NewDataKey(method, uint64(123))
	require.NoError(t, err)
	length, err := KeyLength(method)
	require.NoError(t, err)
	require.Len(t, key.Key, length)
	require.Equal(t, method, key.Method)
	require.False(t, key.WasExposed)
	require.Equal(t, uint64(123), key.CreationTime)
}

func TestNewDataKey(t *testing.T) {
	testNewDataKey(t, encryptionpb.EncryptionMethod_AES128_CTR)
	testNewDataKey(t, encryptionpb.EncryptionMethod_AES192_CTR)
	testNewDataKey(t, encryptionpb.EncryptionMethod_AES256_CTR)
}

func TestAesGcmCrypter(t *testing.T) {
	key, err := hex.DecodeString("ed568fbd8c8018ed2d042a4e5d38d6341486922d401d2022fb81e47c900d3f07")
	require.NoError(t, err)
	plaintext, err := hex.DecodeString(
		"5c873a18af5e7c7c368cb2635e5a15c7f87282085f4b991e84b78c5967e946d4")
	require.NoError(t, err)
	// encrypt
	ivBytes, err := hex.DecodeString("ba432b70336c40c39ba14c1b")
	require.NoError(t, err)
	iv := IvGCM(ivBytes)
	ciphertext, err := aesGcmEncryptImpl(key, plaintext, iv)
	require.NoError(t, err)
	require.Len(t, []byte(iv), ivLengthGCM)
	require.Equal(
		t,
		"bbb9b49546350880cf55d4e4eaccc831c506a4aeae7f6cda9c821d4cb8cfc269dcdaecb09592ef25d7a33b40d3f02208",
		hex.EncodeToString(ciphertext),
	)
	// decrypt
	plaintext2, err := AesGcmDecrypt(key, ciphertext, iv)
	require.NoError(t, err)
	require.True(t, bytes.Equal(plaintext2, plaintext))
	// Modify ciphertext to test authentication failure. We modify the beginning of the ciphertext,
	// which is the real ciphertext part, not the tag.
	fakeCiphertext := make([]byte, len(ciphertext))
	copy(fakeCiphertext, ciphertext)
	// ignore overflow
	fakeCiphertext[0] = ciphertext[0] + 1
	_, err = AesGcmDecrypt(key, fakeCiphertext, iv)
	require.NotNil(t, err)
}
