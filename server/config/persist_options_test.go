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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core/storelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
)

func TestPeekStoreLimitPreservesTTLWithoutCreatingEntry(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, ttl := range []map[string]string{
			nil,
			{"default-add-peer": "31", "default-remove-peer": "37"},
			{"add-peer-1": "41", "remove-peer-1": "43"},
		} {
			re := require.New(t)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			o := &PersistOptions{ttl: cache.NewStringTTL(ctx, time.Minute, time.Hour)}
			cfg := &sc.ScheduleConfig{StoreLimit: make(map[uint64]sc.StoreLimitConfig)}
			if existing {
				cfg.StoreLimit[1] = sc.StoreLimitConfig{AddPeer: 11, RemovePeer: 13}
			}
			o.SetScheduleConfig(cfg)
			for key, value := range ttl {
				o.ttl.Put(key, value)
			}
			add := o.PeekStoreLimitByType(1, storelimit.AddPeer)
			remove := o.PeekStoreLimitByType(1, storelimit.RemovePeer)
			_, ok := o.GetScheduleConfig().StoreLimit[1]
			re.Equal(existing, ok)
			re.Equal(o.GetStoreLimitByType(1, storelimit.AddPeer), add)
			re.Equal(o.GetStoreLimitByType(1, storelimit.RemovePeer), remove)
			cancel()
		}
	}
}
