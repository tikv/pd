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
	"context"
	"os"

	sdkconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/pingcap/kvproto/pkg/encryptionpb"

	"github.com/tikv/pd/pkg/errs"
)

const (
	// We only support AWS KMS right now.
	kmsVendorAWS = "AWS"

	// K8S IAM related environment variables.
	envAwsRoleArn = "AWS_ROLE_ARN"
	// #nosec
	envAwsWebIdentityTokenFile = "AWS_WEB_IDENTITY_TOKEN_FILE"
	envAwsRoleSessionName      = "AWS_ROLE_SESSION_NAME"
)

func newMasterKeyFromKMS(
	config *encryptionpb.MasterKeyKms,
	ciphertextKey []byte,
) (masterKey *MasterKey, err error) {
	if config == nil {
		return nil, errs.ErrEncryptionNewMasterKey.GenWithStack("missing master key file config")
	}
	if config.Vendor != kmsVendorAWS {
		return nil, errs.ErrEncryptionKMS.GenWithStack("unsupported KMS vendor: %s", config.Vendor)
	}

	cfg, err := sdkconfig.LoadDefaultConfig(context.TODO(),
		sdkconfig.WithRegion(config.Region),
	)
	if err != nil {
		return nil, errs.ErrEncryptionKMS.Wrap(err).GenWithStack(
			"fail to load default config")
	}

	// Credentials from K8S IAM role.
	roleArn := os.Getenv(envAwsRoleArn)
	tokenFile := os.Getenv(envAwsWebIdentityTokenFile)
	sessionName := os.Getenv(envAwsRoleSessionName)
	optFn := func(*kms.Options) {}
	// Session name is optional.
	if roleArn != "" && tokenFile != "" {
		client := sts.NewFromConfig(cfg)
		webIdentityRoleProvider := stscreds.NewWebIdentityRoleProvider(
			client,
			roleArn,
			stscreds.IdentityTokenFile(tokenFile),
			func(o *stscreds.WebIdentityRoleOptions) {
				o.RoleSessionName = sessionName
			},
		)
		optFn = func(options *kms.Options) {
			options.Credentials = webIdentityRoleProvider
		}
	}
	client := kms.NewFromConfig(cfg, optFn)
	if len(ciphertextKey) == 0 {
		numberOfBytes := int32(masterKeyLength)
		// Create a new data key.
		output, err := client.GenerateDataKey(context.Background(), &kms.GenerateDataKeyInput{
			KeyId:         &config.KeyId,
			NumberOfBytes: &numberOfBytes,
		})
		if err != nil {
			return nil, errs.ErrEncryptionKMS.Wrap(err).GenWithStack(
				"fail to generate data key from AWS KMS")
		}
		if len(output.Plaintext) != masterKeyLength {
			return nil, errs.ErrEncryptionKMS.GenWithStack(
				"unexpected data key length generated from AWS KMS, expected %d vs actual %d",
				masterKeyLength, len(output.Plaintext))
		}
		masterKey = &MasterKey{
			key:           output.Plaintext,
			ciphertextKey: output.CiphertextBlob,
		}
	} else {
		// Decrypt existing data key.
		output, err := client.Decrypt(context.Background(), &kms.DecryptInput{
			KeyId:          &config.KeyId,
			CiphertextBlob: ciphertextKey,
		})
		if err != nil {
			return nil, errs.ErrEncryptionKMS.Wrap(err).GenWithStack(
				"fail to decrypt data key from AWS KMS")
		}
		if len(output.Plaintext) != masterKeyLength {
			return nil, errs.ErrEncryptionKMS.GenWithStack(
				"unexpected data key length decrypted from AWS KMS, expected %d vs actual %d",
				masterKeyLength, len(output.Plaintext))
		}
		masterKey = &MasterKey{
			key:           output.Plaintext,
			ciphertextKey: ciphertextKey,
		}
	}
	return
}
