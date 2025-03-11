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

package endpoint

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestInitClusterID(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	defer clean()

	id, err := getClusterIDFromEtcd(client)
	re.NoError(err)
	re.Equal(uint64(0), id)
	re.Equal(uint64(0), keypath.ClusterID())

	clusterID, err := InitClusterID(client)
	re.NoError(err)
	re.NotZero(clusterID)
	re.Equal(clusterID, keypath.ClusterID())

	clusterID1, err := InitClusterID(client)
	re.NoError(err)
	re.Equal(clusterID, clusterID1)

	id, err = getClusterIDFromEtcd(client)
	re.NoError(err)
	re.Equal(clusterID, id)
	re.Equal(clusterID, keypath.ClusterID())
}
