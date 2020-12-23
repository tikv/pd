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
	"encoding/hex"
	"io/ioutil"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

func TestMasterKey(t *testing.T) {
	TestingT(t)
}

type testMasterKeySuite struct{}

var _ = Suite(&testMasterKeySuite{})

func (s *testMasterKeySuite) TestPlaintextMasterKey(c *C) {
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_Plaintext{
			Plaintext: &encryptionpb.MasterKeyPlaintext{},
		},
	}
	masterKey, err := NewMasterKey(config, nil)
	c.Assert(err, IsNil)
	c.Assert(masterKey, Not(IsNil))
	c.Assert(len(masterKey.key), Equals, 0)

	plaintext := "this is a plaintext"
	ciphertext, iv, err := masterKey.Encrypt([]byte(plaintext))
	c.Assert(err, IsNil)
	c.Assert(len(iv), Equals, 0)
	c.Assert(string(ciphertext), Equals, plaintext)

	plaintext2, err := masterKey.Decrypt(ciphertext, iv)
	c.Assert(err, IsNil)
	c.Assert(string(plaintext2), Equals, plaintext)

	c.Assert(masterKey.IsPlaintext(), IsTrue)
}

func (s *testMasterKeySuite) TestEncrypt(c *C) {
	keyHex := "2f07ec61e5a50284f47f2b402a962ec672e500b26cb3aa568bb1531300c74806"
	key, err := hex.DecodeString(keyHex)
	c.Assert(err, IsNil)
	masterKey := &MasterKey{key: key}
	plaintext := "this-is-a-plaintext"
	ciphertext, iv, err := masterKey.Encrypt([]byte(plaintext))
	c.Assert(err, IsNil)
	c.Assert(len(iv), Equals, ivLengthGCM)
	plaintext2, err := AesGcmDecrypt(key, ciphertext, iv)
	c.Assert(err, IsNil)
	c.Assert(string(plaintext2), Equals, plaintext)
}

func (s *testMasterKeySuite) TestDecrypt(c *C) {
	keyHex := "2f07ec61e5a50284f47f2b402a962ec672e500b26cb3aa568bb1531300c74806"
	key, err := hex.DecodeString(keyHex)
	c.Assert(err, IsNil)
	plaintext := "this-is-a-plaintext"
	iv, err := hex.DecodeString("ba432b70336c40c39ba14c1b")
	c.Assert(err, IsNil)
	ciphertext, err := aesGcmEncryptImpl(key, []byte(plaintext), iv)
	c.Assert(err, IsNil)
	masterKey := &MasterKey{key: key}
	plaintext2, err := masterKey.Decrypt(ciphertext, iv)
	c.Assert(err, IsNil)
	c.Assert(string(plaintext2), Equals, plaintext)
}

func (s *testMasterKeySuite) TestNewFileMasterKeyMissingPath(c *C) {
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: "",
			},
		},
	}
	_, err := NewMasterKey(config, nil)
	c.Assert(err, Not(IsNil))
}

func (s *testMasterKeySuite) TestNewFileMasterKeyMissingFile(c *C) {
	dir, err := ioutil.TempDir("", "test_key_files")
	c.Assert(err, IsNil)
	path := dir + "/key"
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: path,
			},
		},
	}
	_, err = NewMasterKey(config, nil)
	c.Assert(err, Not(IsNil))
}

func (s *testMasterKeySuite) TestNewFileMasterKeyNotHexString(c *C) {
	dir, err := ioutil.TempDir("", "test_key_files")
	c.Assert(err, IsNil)
	path := dir + "/key"
	ioutil.WriteFile(path, []byte("not-a-hex-string"), 0644)
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: path,
			},
		},
	}
	_, err = NewMasterKey(config, nil)
	c.Assert(err, Not(IsNil))
}

func (s *testMasterKeySuite) TestNewFileMasterKeyLengthMismatch(c *C) {
	dir, err := ioutil.TempDir("", "test_key_files")
	c.Assert(err, IsNil)
	path := dir + "/key"
	ioutil.WriteFile(path, []byte("2f07ec61e5a50284f47f2b402a962ec6"), 0644)
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: path,
			},
		},
	}
	_, err = NewMasterKey(config, nil)
	c.Assert(err, Not(IsNil))
}

func (s *testMasterKeySuite) TestNewFileMasterKey(c *C) {
	key := "2f07ec61e5a50284f47f2b402a962ec672e500b26cb3aa568bb1531300c74806"
	dir, err := ioutil.TempDir("", "test_key_files")
	c.Assert(err, IsNil)
	path := dir + "/key"
	ioutil.WriteFile(path, []byte(key), 0644)
	config := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: path,
			},
		},
	}
	masterKey, err := NewMasterKey(config, nil)
	c.Assert(err, IsNil)
	c.Assert(hex.EncodeToString(masterKey.key), Equals, key)
}
