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

package server

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
)

func TestLabelPropertyWarning(t *testing.T) {
	const warning = "label-property is deprecated and the scheduling service ignores it, use placement rules or evict-leader-scheduler instead"
	const persistFail = "github.com/tikv/pd/server/config/persistFail"
	operations := []struct {
		name string
		run  func(*Server) error
		want config.LabelPropertyConfig
	}{
		{
			name: "set",
			run: func(s *Server) error {
				return s.SetLabelProperty("reject-leader", "zone", "new")
			},
			want: config.LabelPropertyConfig{"reject-leader": {{Key: "zone", Value: "old"}, {Key: "zone", Value: "new"}}},
		},
		{
			name: "config",
			run: func(s *Server) error {
				return s.SetLabelPropertyConfig(config.LabelPropertyConfig{"reject-leader": {{Key: "rack", Value: "replacement"}}})
			},
			want: config.LabelPropertyConfig{"reject-leader": {{Key: "rack", Value: "replacement"}}},
		},
		{
			name: "delete",
			run: func(s *Server) error {
				return s.DeleteLabelProperty("reject-leader", "zone", "old")
			},
			want: config.LabelPropertyConfig{},
		},
	}
	// These cases replace the global logger and enable a global failpoint.
	// Keep them sequential so each operation observes only its own logs/failure.
	for _, independent := range []bool{false, true} {
		for _, op := range operations {
			for _, failPersist := range []bool{false, true} {
				t.Run(fmt.Sprintf("independent=%t/%s/persist_failure=%t", independent, op.name, failPersist), func(t *testing.T) {
					re := require.New(t)
					before := config.LabelPropertyConfig{"reject-leader": {{Key: "zone", Value: "old"}}}
					cfg := config.NewConfig()
					re.NoError(cfg.Adjust(nil, false))
					cfg.LabelProperty = before.Clone()
					store := storage.NewStorageWithMemoryBackend()
					s := &Server{
						persistOptions:         config.NewPersistOptions(cfg),
						storage:                store,
						cluster:                &cluster.RaftCluster{},
						isKeyspaceGroupEnabled: true,
					}
					s.isRunning.Store(1)
					if independent {
						s.cluster.SetServiceIndependent(mcs.SchedulingServiceName)
					}
					re.Equal(independent, s.IsServiceIndependent(mcs.SchedulingServiceName))
					re.NoError(s.persistOptions.Persist(store))
					core, logs := observer.New(zapcore.WarnLevel)
					t.Cleanup(log.ReplaceGlobals(zap.New(core), nil))
					if failPersist {
						re.NoError(failpoint.Enable(persistFail, "return(true)"))
						t.Cleanup(func() { re.NoError(failpoint.Disable(persistFail)) })
					}

					err := op.run(s)
					want := op.want
					if failPersist {
						re.EqualError(err, "fail to persist")
						want = before
					} else {
						re.NoError(err)
					}
					re.Equal(want, s.GetLabelProperty())
					reloaded := config.NewPersistOptions(config.NewConfig())
					re.NoError(reloaded.Reload(store))
					re.Equal(want, reloaded.GetLabelPropertyConfig())

					warnings := logs.FilterMessage(warning).All()
					if independent && op.name != "delete" && !failPersist {
						re.Len(warnings, 1)
						re.Equal(zapcore.WarnLevel, warnings[0].Level)
						re.Equal(want, warnings[0].ContextMap()["config"])
					} else {
						re.Empty(warnings)
					}
				})
			}
		}
	}
}
