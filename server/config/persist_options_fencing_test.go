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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/errs"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

type reloadBeforeConfigSave struct {
	endpoint.ConfigStorage
	reload func()
}

func (s *reloadBeforeConfigSave) SaveConfig(any) error {
	s.reload()
	return errs.ErrEtcdTxnConflict.FastGenByArgs()
}

func TestUpdateScheduleConfigPreservesReload(t *testing.T) {
	for _, duringSave := range []bool{false, true} {
		name := "before-publication"
		if duringSave {
			name = "before-rollback"
		}
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			cfg := NewConfig()
			re.NoError(cfg.Adjust(nil, false))
			options := NewPersistOptions(cfg)
			source := storage.NewStorageWithMemoryBackend()
			newer := NewConfig()
			re.NoError(newer.Adjust(nil, false))
			newer.Schedule.LeaderScheduleLimit = 42
			re.NoError(NewPersistOptions(newer).Persist(source))
			reload := func() { re.NoError(options.Reload(source)) }
			delayed := &reloadBeforeConfigSave{ConfigStorage: source, reload: reload}
			err := options.UpdateScheduleConfig(delayed, func(_, next *sc.ScheduleConfig) (bool, error) {
				next.LeaderScheduleLimit = 99
				if !duringSave {
					reload()
				}
				return true, nil
			})
			re.ErrorIs(err, errs.ErrEtcdTxnConflict)
			re.Equal(uint64(42), options.GetLeaderScheduleLimit())
			// A subsequent current-term persistence must keep the reloaded value.
			re.NoError(options.Persist(source))
			loaded := NewPersistOptions(NewConfig())
			re.NoError(loaded.Reload(source))
			re.Equal(uint64(42), loaded.GetLeaderScheduleLimit())
		})
	}
}
