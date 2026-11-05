// Copyright 2026 TiKV Project Authors.
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

package config

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/pingcap/errors"
)

const grpcGatewayDefaultFile = "grpc-gateway-default"

// grpcGatewayDefault preserves the old default for initialized members. New
// members record the new default before etcd creates its WAL, so restarting them
// does not enable the gateway. An explicit configuration overrides this default.
func (c *Config) grpcGatewayDefault() (bool, error) {
	data, err := os.ReadFile(filepath.Join(c.DataDir, grpcGatewayDefaultFile))
	if err == nil {
		var enabled *bool
		if err = json.Unmarshal(data, &enabled); err != nil {
			return false, errors.Annotate(err, "read gRPC gateway default")
		}
		if enabled == nil {
			return false, errors.New("gRPC gateway default must be a boolean")
		}
		return *enabled, nil
	}
	if !os.IsNotExist(err) {
		return false, errors.Annotate(err, "read gRPC gateway default")
	}
	info, err := os.Stat(filepath.Join(c.DataDir, "member", "wal"))
	if err == nil {
		if !info.IsDir() {
			return false, errors.New("etcd WAL path is not a directory")
		}
		return true, nil
	}
	if !os.IsNotExist(err) {
		return false, errors.Annotate(err, "check existing etcd member")
	}
	return defaultEnableGRPCGateway, nil
}

// PersistGRPCGatewayDefault records the member's default before starting etcd.
// It does not persist an explicit override from the configuration file.
func (c *Config) PersistGRPCGatewayDefault() error {
	path := filepath.Join(c.DataDir, grpcGatewayDefaultFile)
	if _, err := os.Stat(path); err == nil {
		return syncGRPCGatewayDefaultDir(c.DataDir)
	} else if !os.IsNotExist(err) {
		return errors.Annotate(err, "check gRPC gateway default")
	}
	enabled, err := c.grpcGatewayDefault()
	if err != nil {
		return err
	}
	if err = os.MkdirAll(c.DataDir, 0700); err != nil {
		return errors.WithStack(err)
	}
	file, err := os.CreateTemp(c.DataDir, ".grpc-gateway-default-*")
	if err != nil {
		return errors.WithStack(err)
	}
	defer os.Remove(file.Name())
	if err = json.NewEncoder(file).Encode(enabled); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return errors.WithStack(err)
	}
	if closeErr != nil {
		return errors.WithStack(closeErr)
	}
	if err = os.Rename(file.Name(), path); err != nil {
		return errors.WithStack(err)
	}
	// Make the rename durable before etcd can create a WAL. Otherwise a crash
	// could leave a new member looking like an upgraded member on restart.
	return syncGRPCGatewayDefaultDir(c.DataDir)
}

func syncGRPCGatewayDefaultDir(dataDir string) error {
	dir, err := os.Open(dataDir)
	if err != nil {
		return errors.WithStack(err)
	}
	err = dir.Sync()
	closeErr := dir.Close()
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(closeErr)
}
