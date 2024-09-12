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
	"crypto/rand"
	"encoding/binary"
	"io"

	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/tikv/pd/pkg/errs"
)

const (
	ivLengthCTR  = 16
	ivLengthGCM  = 12
	keyIDBufSize = 8
)

// CheckEncryptionMethodSupported check whether the encryption method is currently supported.
// This is to handle future extension to encryption methods on kvproto side.
func CheckEncryptionMethodSupported(method encryptionpb.EncryptionMethod) error {
	switch method {
	case encryptionpb.EncryptionMethod_AES128_CTR:
		return nil
	case encryptionpb.EncryptionMethod_AES192_CTR:
		return nil
	case encryptionpb.EncryptionMethod_AES256_CTR:
		return nil
	default:
		name, ok := encryptionpb.EncryptionMethod_name[int32(method)]
		if ok {
			return errs.ErrEncryptionInvalidMethod.GenWithStackByArgs(name)
		}
		return errs.ErrEncryptionInvalidMethod.GenWithStackByArgs(int32(method))
	}
}

// KeyLength return the encryption key length for supported encryption methods.
func KeyLength(method encryptionpb.EncryptionMethod) (int, error) {
	switch method {
	case encryptionpb.EncryptionMethod_AES128_CTR:
		return 16, nil
	case encryptionpb.EncryptionMethod_AES192_CTR:
		return 24, nil
	case encryptionpb.EncryptionMethod_AES256_CTR:
		return 32, nil
	default:
		name, ok := encryptionpb.EncryptionMethod_name[int32(method)]
		if ok {
			return 0, errs.ErrEncryptionInvalidMethod.GenWithStackByArgs(name)
		}
		return 0, errs.ErrEncryptionInvalidMethod.GenWithStackByArgs(int32(method))
	}
}

// IvCTR represent IV bytes for CTR mode.
type IvCTR []byte

// IvGCM represent IV bytes for GCM mode.
type IvGCM []byte

func newIV(ivLength int) ([]byte, error) {
	iv := make([]byte, ivLength)
	n, err := io.ReadFull(rand.Reader, iv)
	if err != nil {
		return nil, errs.ErrEncryptionGenerateIV.Wrap(err).GenWithStackByArgs()
	}
	if n != ivLength {
		return nil, errs.ErrEncryptionGenerateIV.GenWithStack(
			"iv length expected %d vs actual %d", ivLength, n)
	}
	return iv, nil
}

// NewIvCTR randomly generate an IV for CTR mode.
func NewIvCTR() (IvCTR, error) {
	return newIV(ivLengthCTR)
}

// NewIvGCM randomly generate an IV for GCM mode.
func NewIvGCM() (IvGCM, error) {
	return newIV(ivLengthGCM)
}

// NewDataKey randomly generate a new data key.
func NewDataKey(
	method encryptionpb.EncryptionMethod,
	creationTime uint64,
) (keyID uint64, key *encryptionpb.DataKey, err error) {
	err = CheckEncryptionMethodSupported(method)
	if err != nil {
		return
	}

	keyIDBuf := make([]byte, keyIDBufSize)
	n, err := io.ReadFull(rand.Reader, keyIDBuf)
	if err != nil {
		err = errs.ErrEncryptionNewDataKey.Wrap(err).GenWithStack(
			"fail to generate data key id")
		return
	}
	if n != keyIDBufSize {
		err = errs.ErrEncryptionNewDataKey.GenWithStack(
			"no enough random bytes to generate data key id, bytes %d", n)
		return
	}
	keyID = binary.BigEndian.Uint64(keyIDBuf)
	keyLength, err := KeyLength(method)
	if err != nil {
		return
	}
	keyBuf := make([]byte, keyLength)
	n, err = io.ReadFull(rand.Reader, keyBuf)
	if err != nil {
		err = errs.ErrEncryptionNewDataKey.Wrap(err).GenWithStack(
			"fail to generate data key")
		return
	}
	if n != keyLength {
		err = errs.ErrEncryptionNewDataKey.GenWithStack(
			"no enough random bytes to generate data key, bytes %d", n)
		return
	}
	key = &encryptionpb.DataKey{
		Key:          keyBuf,
		Method:       method,
		CreationTime: creationTime,
		WasExposed:   false,
	}
	return
}

func aesGcmEncryptImpl(
	key []byte,
	plaintext []byte,
	iv IvGCM,
) (ciphertext []byte, err error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		err = errs.ErrEncryptionGCMEncrypt.Wrap(err).GenWithStack("fail to create aes cipher")
		return
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		err = errs.ErrEncryptionGCMEncrypt.Wrap(err).GenWithStack("fail to create aes-gcm cipher")
		return
	}
	ciphertext = aesgcm.Seal(nil, iv, plaintext, nil)
	return
}

// AesGcmEncrypt encrypt given plaintext with given key using aes256-gcm.
// The method is used to encrypt data keys.
func AesGcmEncrypt(
	key []byte,
	plaintext []byte,
) (ciphertext []byte, iv IvGCM, err error) {
	iv, err = NewIvGCM()
	if err != nil {
		return
	}
	ciphertext, err = aesGcmEncryptImpl(key, plaintext, iv)
	return
}

// AesGcmDecrypt decrypt given ciphertext with given key using aes256-gcm.
// The method is used to decrypt data keys.
func AesGcmDecrypt(
	key []byte,
	ciphertext []byte,
	iv IvGCM,
) (plaintext []byte, err error) {
	if len(iv) != ivLengthGCM {
		err = errs.ErrEncryptionGCMDecrypt.GenWithStack("unexpected gcm iv length %d", len(iv))
		return
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		err = errs.ErrEncryptionGCMDecrypt.Wrap(err).GenWithStack("fail to create aes cipher")
		return
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		err = errs.ErrEncryptionGCMDecrypt.Wrap(err).GenWithStack("fail to create aes-gcm cipher")
		return
	}
	plaintext, err = aesgcm.Open(nil, iv, ciphertext, nil)
	if err != nil {
		err = errs.ErrEncryptionGCMDecrypt.Wrap(err).GenWithStack("authentication fail")
		return
	}
	return
}
