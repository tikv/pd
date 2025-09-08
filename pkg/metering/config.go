// Copyright 2025 TiKV Project Authors.
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

package metering

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/metering_sdk/storage"
)

// Config represents the configuration for metering.
type Config struct {
	Type    storage.ProviderType `yaml:"type,omitempty" toml:"type,omitempty" json:"type,omitempty" reloadable:"false"`
	Region  string               `yaml:"region,omitempty" toml:"region,omitempty" json:"region,omitempty" reloadable:"false"`
	Bucket  string               `yaml:"bucket,omitempty" toml:"bucket,omitempty" json:"bucket,omitempty" reloadable:"false"`
	Prefix  string               `yaml:"prefix,omitempty" toml:"prefix,omitempty" json:"prefix,omitempty" reloadable:"false"`
	RoleARN string               `yaml:"role-arn,omitempty" toml:"role-arn,omitempty" json:"role-arn,omitempty" reloadable:"false"`
}

func (c *Config) adjust() error {
	if len(c.Type) == 0 {
		c.Type = storage.ProviderTypeS3
	}
	if len(c.Region) == 0 {
		return errors.New("region is required for the metering config")
	}
	if len(c.Bucket) == 0 {
		return errors.New("bucket is required for the metering config")
	}
	if c.Type == storage.ProviderTypeS3 && len(c.RoleARN) == 0 {
		return errors.New("role-arn is required for the metering config when using S3")
	}
	return nil
}
