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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiagnosticsJSONDistinguishesEmptyAndUnknown(t *testing.T) {
	re := require.New(t)
	d := GroupDiagnostics{
		ActualLeaderStoreIDs: make([]uint64, 0),
		ActualVoterStoreIDs:  make([][]uint64, 0),
		RunningOperators:     make([]OperatorCount, 0),
		BlockedReasons:       make([]BlockedReason, 0),
	}
	data, err := json.Marshal(d)
	re.NoError(err)
	re.JSONEq(`{
		"effective_policy":null,
		"observed_at":"",
		"actual_leader_store_ids":[],
		"actual_voter_store_ids":[],
		"approximate_size_mib":null,
		"pending_move_mib":null,
		"leader_spread":null,
		"voter_spread":null,
		"running_operators":[],
		"blocked_reasons":[]
	}`, string(data))

	d.ActualLeaderStoreIDs = nil
	d.ActualVoterStoreIDs = nil
	d.RunningOperators = nil
	d.BlockedReasons = nil
	data, err = json.Marshal(d)
	re.NoError(err)
	re.Contains(string(data), `"actual_leader_store_ids":null`)
	re.Contains(string(data), `"actual_voter_store_ids":null`)
	re.Contains(string(data), `"running_operators":null`)
	re.Contains(string(data), `"blocked_reasons":null`)
}
