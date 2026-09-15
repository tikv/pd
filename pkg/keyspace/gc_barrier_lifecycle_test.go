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

package keyspace

import (
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/storage/endpoint"
)

func (suite *keyspaceTestSuite) TestGCBarrierRemovalInvalidationAfterCommit() {
	re := suite.Require()
	m := suite.manager
	meta := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 20000}, Name: "barrier-remove", State: keyspacepb.KeyspaceState_TOMBSTONE}
	re.NoError(m.saveNewKeyspace(meta))
	re.NoError(m.kgm.CreateKeyspaceGroups([]*endpoint.KeyspaceGroup{{ID: 101, UserKind: endpoint.Standard.String(), Keyspaces: []uint32{20000}}}))
	calls := 0
	m.SetGCBarrierInvalidator(func(id uint32) {
		calls++
		m.metaLock.Lock(id)
		defer m.metaLock.Unlock(id)
		_, err := m.LoadKeyspaceByID(id)
		re.Error(err)
		// A group operation takes the group lock, detecting lock-order regression.
		_, err = m.kgm.GetKeyspaceGroups(101, 1)
		re.NoError(err)
	})
	base := m.kgm.store
	m.kgm.store = &errorKeyspaceGroupStorage{StorageEndpoint: m.store.(*endpoint.StorageEndpoint), failOnSaveID: 101}
	_, err := m.kgm.RemoveKeyspacesFromGroup(101, m, []uint32{20000})
	re.Error(err)
	re.Zero(calls)
	m.kgm.store = base
	_, err = m.kgm.RemoveKeyspacesFromGroup(101, m, []uint32{20000})
	re.NoError(err)
	re.Equal(1, calls)
}
