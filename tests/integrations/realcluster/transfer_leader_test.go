// Copyright 2023 TiKV Authors
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

package realcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// https://github.com/tikv/pd/issues/6988#issuecomment-1694924611
// https://github.com/tikv/pd/issues/6897
func TestTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := pdHTTPCli.GetLeader(ctx)
	re.NoError(err)
	oldLeader := resp.Name

	var newLeader string
	for i := 0; i < 2; i++ {
		if resp.Name != fmt.Sprintf("pd-%d", i) {
			newLeader = fmt.Sprintf("pd-%d", i)
		}
	}

	// record scheduler
	err = pdHTTPCli.CreateScheduler(ctx, "evict-leader-scheduler", 1)
	re.NoError(err)
	res, err := pdHTTPCli.GetSchedulers(ctx)
	re.NoError(err)
	oldSchedulersLen := len(res)

	re.NoError(pdHTTPCli.TransferLeader(ctx, newLeader))
	// wait for transfer leader to new leader
	time.Sleep(1 * time.Second)
	resp, err = pdHTTPCli.GetLeader(ctx)
	re.NoError(err)
	re.Equal(newLeader, resp.Name)

	res, err = pdHTTPCli.GetSchedulers(ctx)
	re.NoError(err)
	re.Equal(oldSchedulersLen, len(res))

	// transfer leader to old leader
	re.NoError(pdHTTPCli.TransferLeader(ctx, oldLeader))
	// wait for transfer leader
	time.Sleep(1 * time.Second)
	resp, err = pdHTTPCli.GetLeader(ctx)
	re.NoError(err)
	re.Equal(oldLeader, resp.Name)

	res, err = pdHTTPCli.GetSchedulers(ctx)
	re.NoError(err)
	re.Equal(oldSchedulersLen, len(res))
}
