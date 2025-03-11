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
	"reflect"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

// processRegionKeys encrypt or decrypt the start key and end key of the region in-place,
// using the given data key and IV.
func processRegionKeys(region *metapb.Region, key *encryptionpb.DataKey, iv []byte) error {
	block, err := aes.NewCipher(key.Key)
	if err != nil {
		return errors.Wrap(err, "fail to create aes cipher")
	}
	stream := cipher.NewCTR(block, iv)
	stream.XORKeyStream(region.StartKey, region.StartKey)
	stream.XORKeyStream(region.EndKey, region.EndKey)
	return nil
}

// EncryptRegion encrypt the region start key and end key, using the current key return from the
// key manager. The return is an encrypted copy of the region, with Encryption meta updated.
func EncryptRegion(region *metapb.Region, keyManager KeyManager) (*metapb.Region, error) {
	if region == nil {
		return nil, errs.ErrEncryptionEncryptRegion.GenWithStack("trying to encrypt nil region")
	}
	if region.EncryptionMeta != nil {
		return nil, errs.ErrEncryptionEncryptRegion.GenWithStack(
			"region already encrypted, region id = %d", region.Id)
	}
	if keyManager == nil ||
		(reflect.TypeOf(keyManager).Kind() == reflect.Ptr && reflect.ValueOf(keyManager).IsNil()) {
		// encryption is not enabled.
		return region, nil
	}
	keyID, key, err := keyManager.GetCurrentKey()
	if err != nil {
		return nil, err
	}
	if key == nil {
		// encryption is not enabled.
		return region, nil
	}
	err = CheckEncryptionMethodSupported(key.Method)
	if err != nil {
		return nil, err
	}
	iv, err := NewIvCTR()
	if err != nil {
		return nil, err
	}
	// Deep copy region before altering it.
	outRegion := typeutil.DeepClone(region, core.RegionFactory)
	// Encrypt and update in-place.
	err = processRegionKeys(outRegion, key, iv)
	if err != nil {
		return nil, err
	}
	outRegion.EncryptionMeta = &encryptionpb.EncryptionMeta{
		KeyId: keyID,
		Iv:    iv,
	}
	return outRegion, nil
}

// DecryptRegion decrypt the region start key and end key, if the region object was encrypted.
// After decryption, encryption meta is also cleared.
// Note: Call may need to make deep copy of the object if changing the object is undesired.
func DecryptRegion(region *metapb.Region, keyManager KeyManager) error {
	if region == nil {
		return errs.ErrEncryptionDecryptRegion.GenWithStack("trying to decrypt nil region")
	}
	if region.EncryptionMeta == nil {
		return nil
	}
	if keyManager == nil ||
		(reflect.TypeOf(keyManager).Kind() == reflect.Ptr && reflect.ValueOf(keyManager).IsNil()) {
		return errs.ErrEncryptionDecryptRegion.GenWithStack(
			"unable to decrypt region without encryption keys")
	}
	key, err := keyManager.GetKey(region.EncryptionMeta.KeyId)
	if err != nil {
		return err
	}
	err = CheckEncryptionMethodSupported(key.Method)
	if err != nil {
		return err
	}
	// Decrypt and update in-place.
	err = processRegionKeys(region, key, region.EncryptionMeta.Iv)
	if err != nil {
		return err
	}
	region.EncryptionMeta = nil
	return nil
}
