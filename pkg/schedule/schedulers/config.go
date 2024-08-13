// Copyright 2024 TiKV Project Authors.
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

package schedulers

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

type schedulerConfig interface {
	save() error
	load(any) error
	init(name string, storage endpoint.ConfigStorage, data any)
}

type baseSchedulerConfig struct {
	name    string
	storage endpoint.ConfigStorage

	// data is the config of the scheduler.
	data any
}

func (b *baseSchedulerConfig) init(name string, storage endpoint.ConfigStorage, data any) {
	b.name = name
	b.storage = storage
	b.data = data
}

func (b *baseSchedulerConfig) save() error {
	data, err := EncodeConfig(b.data)
	failpoint.Inject("persistFail", func() {
		err = errors.New("fail to persist")
	})
	if err != nil {
		return err
	}
	return b.storage.SaveSchedulerConfig(b.name, data)
}

func (b *baseSchedulerConfig) load(v any) error {
	data, err := b.storage.LoadSchedulerConfig(b.name)
	if err != nil {
		return err
	}
	return DecodeConfig([]byte(data), v)
}
