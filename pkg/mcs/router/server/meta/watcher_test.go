// Copyright 2025 TiKV Project Authors.
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

package meta

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

// Ensure WaitLoad failure won't leave background goroutines running.
func TestNewWatcherWaitLoadFailed(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/loadTemporaryFail", "return(3)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/loadTemporaryFail"))
	}()

	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	watcher, err := NewWatcher(context.Background(), client, core.NewBasicCluster())
	re.Error(err)
	re.Nil(watcher)
}
