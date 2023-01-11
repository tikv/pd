// Copyright 2023 TiKV Project Authors.
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

package client_test

import (
	"time"
)

func (suite *clientTestSuite) TestSaveTimestamp() {
	re := suite.Require()

	// ignore the monotonic clock reading
	ts := time.Now().Round(0)
	save := uint64(ts.UnixNano())
	err := suite.client.SaveTimestamp(suite.ctx, "/pd/timestamp", save, true)
	re.NoError(err)
	load, err := suite.client.LoadTimestamp(suite.ctx, "/pd/timestamp")
	re.NoError(err)
	re.Equal(ts, time.Unix(0, int64(load)))
	err = suite.client.SaveTimestamp(suite.ctx, "/pd/timestamp", save, false, load+1)
	re.Error(err)
}
