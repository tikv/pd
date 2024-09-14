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
	"path/filepath"
	"testing"

	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/stretchr/testify/require"
)

func TestPlaintextMasterKey(t *testing.T) {
	re := require.New(t)
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_Plaintext{
			Plaintext: &encryptionpb.MasterKeyPlaintext{},
		},
	}
	masterKey, err := NewMasterKey(config, nil)
	re.NoError(err)
	re.NotNil(masterKey)
	re.Empty(masterKey.key)

	plaintext := "this is a plaintext"
	ciphertext, iv, err := masterKey.Encrypt([]byte(plaintext))
	re.NoError(err)
	re.Empty(iv)
	re.Equal(plaintext, string(ciphertext))

	plaintext2, err := masterKey.Decrypt(ciphertext, iv)
	re.NoError(err)
	re.Equal(plaintext, string(plaintext2))

	re.True(masterKey.IsPlaintext())
}

func TestEncrypt(t *testing.T) {
	re := require.New(t)
	keyHex := "2f07ec61e5a50284f47f2b402a962ec672e500b26cb3aa568bb1531300c74806" // #nosec G101
	key, err := hex.DecodeString(keyHex)
	re.NoError(err)
	masterKey := &MasterKey{key: key}
	plaintext := "this-is-a-plaintext"
	ciphertext, iv, err := masterKey.Encrypt([]byte(plaintext))
	re.NoError(err)
	re.Len(iv, ivLengthGCM)
	plaintext2, err := AesGcmDecrypt(key, ciphertext, iv)
	re.NoError(err)
	re.Equal(plaintext, string(plaintext2))
}

func TestDecrypt(t *testing.T) {
	re := require.New(t)
	keyHex := "2f07ec61e5a50284f47f2b402a962ec672e500b26cb3aa568bb1531300c74806" // #nosec G101
	key, err := hex.DecodeString(keyHex)
	re.NoError(err)
	plaintext := "this-is-a-plaintext"
	iv, err := hex.DecodeString("ba432b70336c40c39ba14c1b")
	re.NoError(err)
	ciphertext, err := aesGcmEncryptImpl(key, []byte(plaintext), iv)
	re.NoError(err)
	masterKey := &MasterKey{key: key}
	plaintext2, err := masterKey.Decrypt(ciphertext, iv)
	re.NoError(err)
	re.Equal(plaintext, string(plaintext2))
}

func TestNewFileMasterKeyMissingPath(t *testing.T) {
	re := require.New(t)
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: "",
			},
		},
	}
	_, err := NewMasterKey(config, nil)
	re.Error(err)
}

func TestNewFileMasterKeyMissingFile(t *testing.T) {
	re := require.New(t)
	path := filepath.Join(t.TempDir(), "key")
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: path,
			},
		},
	}
	_, err := NewMasterKey(config, nil)
	re.Error(err)
}

func TestNewFileMasterKeyNotHexString(t *testing.T) {
	re := require.New(t)
	path := filepath.Join(t.TempDir(), "key")
	os.WriteFile(path, []byte("not-a-hex-string"), 0600)
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: path,
			},
		},
	}
	_, err := NewMasterKey(config, nil)
	re.Error(err)
}

func TestNewFileMasterKeyLengthMismatch(t *testing.T) {
	re := require.New(t)
	path := filepath.Join(t.TempDir(), "key")
	os.WriteFile(path, []byte("2f07ec61e5a50284f47f2b402a962ec6"), 0600)
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: path,
			},
		},
	}
	_, err := NewMasterKey(config, nil)
	re.Error(err)
}

func TestNewFileMasterKey(t *testing.T) {
	re := require.New(t)
	key := "2f07ec61e5a50284f47f2b402a962ec672e500b26cb3aa568bb1531300c74806" // #nosec G101
	path := filepath.Join(t.TempDir(), "key")
	os.WriteFile(path, []byte(key), 0600)
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: path,
			},
		},
	}
	masterKey, err := NewMasterKey(config, nil)
	re.NoError(err)
	re.Equal(key, hex.EncodeToString(masterKey.key))
}
