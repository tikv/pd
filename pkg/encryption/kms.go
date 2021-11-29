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
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/sts"
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
	credentials, err := newAwsCredentials()
	if err != nil {
		return nil, err
	}
	session, err := session.NewSession(&aws.Config{
		Credentials: credentials,
		Region:      &config.Region,
		Endpoint:    &config.Endpoint,
	})
	if err != nil {
		return nil, errs.ErrEncryptionKMS.Wrap(err).GenWithStack(
			"fail to create AWS session to access KMS CMK")
	}
	client := kms.New(session)
	if len(ciphertextKey) == 0 {
		numberOfBytes := int64(masterKeyLength)
		// Create a new data key.
		output, err := client.GenerateDataKey(&kms.GenerateDataKeyInput{
			KeyId:         &config.KeyId,
			NumberOfBytes: &numberOfBytes,
		})
		if err != nil {
			return nil, errs.ErrEncryptionKMS.Wrap(err).GenWithStack(
				"fail to generate data key from AWS KMS")
		}
		if len(output.Plaintext) != masterKeyLength {
			return nil, errs.ErrEncryptionKMS.GenWithStack(
				"unexpected data key length generated from AWS KMS, expectd %d vs actual %d",
				masterKeyLength, len(output.Plaintext))
		}
		masterKey = &MasterKey{
			key:           output.Plaintext,
			ciphertextKey: output.CiphertextBlob,
		}
	} else {
		// Decrypt existing data key.
		output, err := client.Decrypt(&kms.DecryptInput{
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

func newAwsCredentials() (*credentials.Credentials, error) {
	var providers []credentials.Provider

	// Credentials from K8S IAM role.
	roleArn := os.Getenv(envAwsRoleArn)
	tokenFile := os.Getenv(envAwsWebIdentityTokenFile)
	sessionName := os.Getenv(envAwsRoleSessionName)
	// Session name is optional.
	if roleArn != "" && tokenFile != "" {
		session, err := session.NewSession()
		if err != nil {
			return nil, errs.ErrEncryptionKMS.Wrap(err).GenWithStack(
				"fail to create AWS session to create a WebIdentityRoleProvider")
		}
		webIdentityProvider := stscreds.NewWebIdentityRoleProvider(
			sts.New(session), roleArn, sessionName, tokenFile)
		providers = append(providers, webIdentityProvider)
	}

	providers = append(providers,
		// Credentials from AWS environment variables.
		&credentials.EnvProvider{},
		// Credentials from default AWS credentials file.
		&credentials.SharedCredentialsProvider{
			Filename: "",
			Profile:  "",
		},
	)

	credentials := credentials.NewChainCredentials(providers)
	return credentials, nil
}
