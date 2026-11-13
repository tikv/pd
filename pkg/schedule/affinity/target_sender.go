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

package affinity

import (
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/logutil"
)

// sendObservedTarget permits one outstanding first-win write per Manager. Busy
// observations are discarded; patrol observes the groups again. A failed write
// delays all new attempts, including observations from other groups.
func (m *Manager) sendObservedTarget(target GroupTarget) {
	m.targetWriteMutex.Lock()
	if m.ctx.Err() != nil || m.targetWriteInFlight || time.Now().Before(m.targetWriteRetryAt) {
		m.targetWriteMutex.Unlock()
		return
	}
	m.targetWriteInFlight = true
	m.targetWriteMutex.Unlock()
	go func() {
		defer logutil.LogPanic()
		_, err := m.targetWriter.WriteGroupTargets(m.ctx, []GroupTarget{target})
		m.targetWriteMutex.Lock()
		m.targetWriteInFlight = false
		if err != nil {
			m.targetWriteRetryAt = time.Now().Add(time.Second)
		}
		m.targetWriteMutex.Unlock()
		if err != nil && m.ctx.Err() == nil {
			targetWriteFailureCounter.Inc()
			log.Warn("failed to write affinity group target", zap.String("group-id", target.Expected.ID), zap.Error(err))
		}
	}()
}
