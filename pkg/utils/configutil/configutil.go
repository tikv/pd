// Copyright 2023 TiKV Project Authors.
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

package configutil

import (
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

// ConfigMetaData is an utility to test if a configuration is defined.
type ConfigMetaData struct {
	meta *toml.MetaData
	path []string
}

// NewConfigMetadata is the a factory method to create a ConfigMetaData object
func NewConfigMetadata(meta *toml.MetaData) *ConfigMetaData {
	return &ConfigMetaData{meta: meta}
}

// IsDefined checks if the given key is defined in the configuration
func (m *ConfigMetaData) IsDefined(key string) bool {
	if m.meta == nil {
		return false
	}
	keys := append([]string(nil), m.path...)
	keys = append(keys, key)
	return m.meta.IsDefined(keys...)
}

// Child gets the config metadata of the given path
func (m *ConfigMetaData) Child(path ...string) *ConfigMetaData {
	newPath := append([]string(nil), m.path...)
	newPath = append(newPath, path...)
	return &ConfigMetaData{
		meta: m.meta,
		path: newPath,
	}
}

// CheckUndecoded checks if the configuration contains undefined items
func (m *ConfigMetaData) CheckUndecoded() error {
	if m.meta == nil {
		return nil
	}
	undecoded := m.meta.Undecoded()
	if len(undecoded) == 0 {
		return nil
	}
	errInfo := "Config contains undefined item: "
	for _, key := range undecoded {
		errInfo += key.String() + ", "
	}
	return errors.New(errInfo[:len(errInfo)-2])
}

// SecurityConfig indicates the security configuration
type SecurityConfig struct {
	grpcutil.TLSConfig
	// RedactInfoLog indicates that whether to enable the log redaction. It can be the following values:
	//   - false: disable redact log.
	//   - true: enable redact log, which will replace the sensitive information with "?".
	//   - "MARKER": enable redact log, which will use single guillemets ‹› to enclose the sensitive information.
	RedactInfoLog logutil.RedactInfoLogType `toml:"redact-info-log" json:"redact-info-log"`
	Encryption    encryption.Config         `toml:"encryption" json:"encryption"`
}

// PrintConfigCheckMsg prints the message about configuration checks.
func PrintConfigCheckMsg(w io.Writer, warningMsgs []string) {
	if len(warningMsgs) == 0 {
		fmt.Fprintln(w, "config check successful")
		return
	}

	for _, msg := range warningMsgs {
		fmt.Fprintln(w, msg)
	}
}

// ConfigFromFile loads config from file.
func ConfigFromFile(c any, path string) (*toml.MetaData, error) {
	meta, err := toml.DecodeFile(path, c)
	return &meta, errors.WithStack(err)
}

// AdjustCommandLineString adjusts the value of a string variable from command line flags.
func AdjustCommandLineString(flagSet *pflag.FlagSet, v *string, name string) {
	if value, _ := flagSet.GetString(name); value != "" {
		*v = value
	}
}

// AdjustCommandLineBool adjusts the value of a bool variable from command line flags.
func AdjustCommandLineBool(flagSet *pflag.FlagSet, v *bool, name string) {
	if value, _ := flagSet.GetBool(name); value {
		*v = value
	}
}

// AdjustString adjusts the value of a string variable.
func AdjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

// AdjustUint64 adjusts the value of a uint64 variable.
func AdjustUint64(v *uint64, defValue uint64) {
	if *v == 0 {
		*v = defValue
	}
}

// AdjustInt64 adjusts the value of a int64 variable.
func AdjustInt64(v *int64, defValue int64) {
	if *v == 0 {
		*v = defValue
	}
}

// AdjustInt adjusts the value of a int variable.
func AdjustInt(v *int, defValue int) {
	if *v == 0 {
		*v = defValue
	}
}

// AdjustFloat64 adjusts the value of a float64 variable.
func AdjustFloat64(v *float64, defValue float64) {
	if *v == 0 {
		*v = defValue
	}
}

// AdjustDuration adjusts the value of a Duration variable.
func AdjustDuration(v *typeutil.Duration, defValue time.Duration) {
	if v.Duration <= 0 {
		v.Duration = defValue
	}
}

// AdjustByteSize adjusts the value of a ByteSize variable.
func AdjustByteSize(v *typeutil.ByteSize, defValue typeutil.ByteSize) {
	if *v == 0 {
		*v = defValue
	}
}

// AdjustPath adjusts the value of a path variable.
func AdjustPath(p *string) {
	absPath, err := filepath.Abs(*p)
	if err == nil {
		*p = absPath
	}
}

// AdjustBool adjusts the value of a bool variable.
func AdjustBool(v *bool, defValue bool) {
	if !*v {
		*v = defValue
	}
}
