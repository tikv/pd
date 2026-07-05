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

package join

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/tikv/pd/server/config"
)

// TestIsDataExist covers all three branches of the isDataExist helper:
// directory does not exist, exists but is empty, and exists with contents.
func TestIsDataExist(t *testing.T) {
	re := require.New(t)

	// Non-existent path must return false.
	re.False(isDataExist("/nonexistent/path/that/does/not/exist"))

	// An existing but empty directory must return false.
	emptyDir := t.TempDir()
	re.False(isDataExist(emptyDir))

	// An existing directory that contains at least one entry must return true.
	nonEmptyDir := t.TempDir()
	f, err := os.Create(filepath.Join(nonEmptyDir, "test-file"))
	re.NoError(err)
	re.NoError(f.Close())
	re.True(isDataExist(nonEmptyDir))
}

// TestPrepareJoinClusterEmptyJoin verifies that when cfg.Join is empty the
// function is a no-op: it returns nil without contacting any cluster.
func TestPrepareJoinClusterEmptyJoin(t *testing.T) {
	re := require.New(t)
	cfg := &config.Config{}
	re.NoError(PrepareJoinCluster(cfg))
}

// TestPrepareJoinClusterJoinSelf verifies that a node is not allowed to join
// itself (Join == AdvertiseClientUrls).
func TestPrepareJoinClusterJoinSelf(t *testing.T) {
	re := require.New(t)
	cfg := &config.Config{}
	cfg.Join = "http://127.0.0.1:2379"
	cfg.AdvertiseClientUrls = "http://127.0.0.1:2379"

	err := PrepareJoinCluster(cfg)
	re.Error(err)
	re.Contains(err.Error(), "join self is forbidden")
}

// TestPrepareJoinClusterWithExistingDataDir verifies that when the member data
// directory already contains data the function immediately sets the cluster
// state to "existing" with an empty initial-cluster string and returns nil,
// without dialling the join address.
func TestPrepareJoinClusterWithExistingDataDir(t *testing.T) {
	re := require.New(t)

	// Populate <tmpDir>/member/ with a file to simulate an existing data store.
	tmpDir := t.TempDir()
	memberDir := filepath.Join(tmpDir, "member")
	re.NoError(os.MkdirAll(memberDir, privateDirMode))
	f, err := os.Create(filepath.Join(memberDir, "wal-file"))
	re.NoError(err)
	re.NoError(f.Close())

	cfg := &config.Config{}
	cfg.Join = "http://127.0.0.1:2379"
	cfg.AdvertiseClientUrls = "http://127.0.0.1:2380"
	cfg.DataDir = tmpDir

	re.NoError(PrepareJoinCluster(cfg))
	// etcd reads cluster membership from the data directory, so the initial
	// cluster string must be empty and the state flag must be "existing".
	re.Equal("", cfg.InitialCluster)
	re.Equal(embed.ClusterStateFlagExisting, cfg.InitialClusterState)
}
