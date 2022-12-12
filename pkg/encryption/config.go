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
	"time"

	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

const (
	methodPlaintext = "plaintext"
	methodAes128Ctr = "aes128-ctr"
	methodAes192Ctr = "aes192-ctr"
	methodAes256Ctr = "aes256-ctr"

	masterKeyTypePlaintext = "plaintext"
	masterKeyTypeKMS       = "kms"
	masterKeyTypeFile      = "file"

	defaultDataEncryptionMethod  = methodPlaintext
	defaultDataKeyRotationPeriod = "168h" // 7 days
)

// Config define the encryption config structure.
type Config struct {
	// Encryption method to use for PD data.
	DataEncryptionMethod string `toml:"data-encryption-method" json:"data-encryption-method"`
	// Specifies how often PD rotates data encryption key.
	DataKeyRotationPeriod typeutil.Duration `toml:"data-key-rotation-period" json:"data-key-rotation-period"`
	// Specifies master key if encryption is enabled.
	MasterKey MasterKeyConfig `toml:"master-key" json:"master-key"`
}

// Adjust validates the config and sets default values.
func (c *Config) Adjust() error {
	if len(c.DataEncryptionMethod) == 0 {
		c.DataEncryptionMethod = defaultDataEncryptionMethod
	} else if _, err := c.GetMethod(); err != nil {
		return err
	}

	if c.DataKeyRotationPeriod.Duration == 0 {
		duration, err := time.ParseDuration(defaultDataKeyRotationPeriod)
		if err != nil {
			return errs.ErrEncryptionInvalidConfig.Wrap(err).GenWithStack(
				"fail to parse default value of data-key-rotation-period %s",
				defaultDataKeyRotationPeriod)
		}
		c.DataKeyRotationPeriod.Duration = duration
	} else if c.DataKeyRotationPeriod.Duration < 0 {
		return errs.ErrEncryptionInvalidConfig.GenWithStack(
			"negative data-key-rotation-period %d",
			c.DataKeyRotationPeriod.Duration)
	}
	if len(c.MasterKey.Type) == 0 {
		c.MasterKey.Type = masterKeyTypePlaintext
	} else if _, err := c.GetMasterKeyMeta(); err != nil {
		return err
	}
	return nil
}

// GetMethod gets the encryption method.
func (c *Config) GetMethod() (encryptionpb.EncryptionMethod, error) {
	switch c.DataEncryptionMethod {
	case methodPlaintext:
		return encryptionpb.EncryptionMethod_PLAINTEXT, nil
	case methodAes128Ctr:
		return encryptionpb.EncryptionMethod_AES128_CTR, nil
	case methodAes192Ctr:
		return encryptionpb.EncryptionMethod_AES192_CTR, nil
	case methodAes256Ctr:
		return encryptionpb.EncryptionMethod_AES256_CTR, nil
	default:
		return encryptionpb.EncryptionMethod_UNKNOWN,
			errs.ErrEncryptionInvalidMethod.GenWithStack("unknown method")
	}
}

// GetMasterKeyMeta gets metadata of master key.
func (c *Config) GetMasterKeyMeta() (*encryptionpb.MasterKey, error) {
	switch c.MasterKey.Type {
	case masterKeyTypePlaintext:
		return &encryptionpb.MasterKey{
			Backend: &encryptionpb.MasterKey_Plaintext{
				Plaintext: &encryptionpb.MasterKeyPlaintext{},
			},
		}, nil
	case masterKeyTypeKMS:
		return &encryptionpb.MasterKey{
			Backend: &encryptionpb.MasterKey_Kms{
				Kms: &encryptionpb.MasterKeyKms{
					Vendor:   kmsVendorAWS,
					KeyId:    c.MasterKey.KmsKeyID,
					Region:   c.MasterKey.KmsRegion,
					Endpoint: c.MasterKey.KmsEndpoint,
				},
			},
		}, nil
	case masterKeyTypeFile:
		return &encryptionpb.MasterKey{
			Backend: &encryptionpb.MasterKey_File{
				File: &encryptionpb.MasterKeyFile{
					Path: c.MasterKey.FilePath,
				},
			},
		}, nil
	default:
		return nil, errs.ErrEncryptionInvalidConfig.GenWithStack(
			"unrecognized encryption master key type: %s", c.MasterKey.Type)
	}
}

// MasterKeyConfig defines master key config structure.
type MasterKeyConfig struct {
	// Master key type, one of "plaintext", "kms" or "file".
	Type string `toml:"type" json:"type"`

	MasterKeyKMSConfig
	MasterKeyFileConfig
}

// MasterKeyKMSConfig defines a KMS master key config structure.
type MasterKeyKMSConfig struct {
	// KMS CMK key id.
	KmsKeyID string `toml:"key-id" json:"key-id"`
	// KMS region of the CMK.
	KmsRegion string `toml:"region" json:"region"`
	// Custom endpoint to access KMS.
	KmsEndpoint string `toml:"endpoint" json:"endpoint"`
}

// MasterKeyFileConfig defines a file-based master key config structure.
type MasterKeyFileConfig struct {
	// Master key file path.
	FilePath string `toml:"path" json:"path"`
}
