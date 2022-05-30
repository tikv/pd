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
	"encoding/hex"
	"os"
	"testing"

	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/stretchr/testify/require"
)

func TestPlaintextMasterKey(t *testing.T) {
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_Plaintext{
			Plaintext: &encryptionpb.MasterKeyPlaintext{},
		},
	}
	masterKey, err := NewMasterKey(config, nil)
	require.NoError(t, err)
	require.NotNil(t, masterKey)
	require.Len(t, masterKey.key, 0)

	plaintext := "this is a plaintext"
	ciphertext, iv, err := masterKey.Encrypt([]byte(plaintext))
	require.NoError(t, err)
	require.Len(t, iv, 0)
	require.Equal(t, plaintext, string(ciphertext))

	plaintext2, err := masterKey.Decrypt(ciphertext, iv)
	require.NoError(t, err)
	require.Equal(t, plaintext, string(plaintext2))

	require.True(t, masterKey.IsPlaintext())
}

func TestEncrypt(t *testing.T) {
	keyHex := "2f07ec61e5a50284f47f2b402a962ec672e500b26cb3aa568bb1531300c74806"
	key, err := hex.DecodeString(keyHex)
	require.NoError(t, err)
	masterKey := &MasterKey{key: key}
	plaintext := "this-is-a-plaintext"
	ciphertext, iv, err := masterKey.Encrypt([]byte(plaintext))
	require.NoError(t, err)
	require.Len(t, iv, ivLengthGCM)
	plaintext2, err := AesGcmDecrypt(key, ciphertext, iv)
	require.NoError(t, err)
	require.Equal(t, plaintext, string(plaintext2))
}

func TestDecrypt(t *testing.T) {
	keyHex := "2f07ec61e5a50284f47f2b402a962ec672e500b26cb3aa568bb1531300c74806"
	key, err := hex.DecodeString(keyHex)
	require.NoError(t, err)
	plaintext := "this-is-a-plaintext"
	iv, err := hex.DecodeString("ba432b70336c40c39ba14c1b")
	require.NoError(t, err)
	ciphertext, err := aesGcmEncryptImpl(key, []byte(plaintext), iv)
	require.NoError(t, err)
	masterKey := &MasterKey{key: key}
	plaintext2, err := masterKey.Decrypt(ciphertext, iv)
	require.NoError(t, err)
	require.Equal(t, plaintext, string(plaintext2))
}

func TestNewFileMasterKeyMissingPath(t *testing.T) {
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: "",
			},
		},
	}
	_, err := NewMasterKey(config, nil)
	require.Error(t, err)
}

func TestNewFileMasterKeyMissingFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "test_key_files")
	require.NoError(t, err)
	path := dir + "/key"
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: path,
			},
		},
	}
	_, err = NewMasterKey(config, nil)
	require.Error(t, err)
}

func TestNewFileMasterKeyNotHexString(t *testing.T) {
	dir, err := os.MkdirTemp("", "test_key_files")
	require.NoError(t, err)
	path := dir + "/key"
	os.WriteFile(path, []byte("not-a-hex-string"), 0600)
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: path,
			},
		},
	}
	_, err = NewMasterKey(config, nil)
	require.Error(t, err)
}

func TestNewFileMasterKeyLengthMismatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "test_key_files")
	require.NoError(t, err)
	path := dir + "/key"
	os.WriteFile(path, []byte("2f07ec61e5a50284f47f2b402a962ec6"), 0600)
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: path,
			},
		},
	}
	_, err = NewMasterKey(config, nil)
	require.Error(t, err)
}

func TestNewFileMasterKey(t *testing.T) {
	key := "2f07ec61e5a50284f47f2b402a962ec672e500b26cb3aa568bb1531300c74806"
	dir, err := os.MkdirTemp("", "test_key_files")
	require.NoError(t, err)
	path := dir + "/key"
	os.WriteFile(path, []byte(key), 0600)
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: path,
			},
		},
	}
	masterKey, err := NewMasterKey(config, nil)
	require.NoError(t, err)
	require.Equal(t, key, hex.EncodeToString(masterKey.key))
}
