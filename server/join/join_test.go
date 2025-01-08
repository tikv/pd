// Copyright 2016 TiKV Project Authors.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
)

// A PD joins itself.
func TestPDJoinsItself(t *testing.T) {
	re := require.New(t)
	cfg := server.NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	defer testutil.CleanServer(cfg.DataDir)
	cfg.Join = cfg.AdvertiseClientUrls
	re.Error(PrepareJoinCluster(cfg))
}
