package configutil

import (
	"errors"

	"github.com/BurntSushi/toml"
)

// ConfigMetaData is the utility to test if a configuration is defined.
type ConfigMetaData struct {
	meta *toml.MetaData
	path []string
}

// NewConfigMetadata creates a new ConfigMetaData.
func NewConfigMetadata(meta *toml.MetaData) *ConfigMetaData {
	return &ConfigMetaData{meta: meta}
}

// IsDefined checks if the key is defined in the configuration.
func (m *ConfigMetaData) IsDefined(key string) bool {
	if m.meta == nil {
		return false
	}
	keys := append([]string(nil), m.path...)
	keys = append(keys, key)
	return m.meta.IsDefined(keys...)
}

// Child creates a new ConfigMetaData with the path appended.
func (m *ConfigMetaData) Child(path ...string) *ConfigMetaData {
	newPath := append([]string(nil), m.path...)
	newPath = append(newPath, path...)
	return &ConfigMetaData{
		meta: m.meta,
		path: newPath,
	}
}

// CheckUndecoded checks if there are any undefined items in the configuration.
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

// ConfigFromFile loads config from file.
func ConfigFromFile(config any, path string) (*toml.MetaData, error) {
	meta, err := toml.DecodeFile(path, config)
	return &meta, err
}
