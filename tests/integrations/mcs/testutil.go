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

package mcs

import (
	"context"
	"sync"

	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/caller"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
)

// SetupClientWithAPIContext creates a TSO client with api context name for test.
func SetupClientWithAPIContext(
	ctx context.Context, re *require.Assertions, apiCtx pd.APIContext, endpoints []string, opts ...opt.ClientOption,
) pd.Client {
	cli, err := pd.NewClientWithAPIContext(ctx, apiCtx,
		caller.TestComponent,
		endpoints, pd.SecurityOption{}, opts...)
	re.NoError(err)
	return cli
}

// SetupClientWithKeyspaceID creates a TSO client with the given keyspace id for test.
func SetupClientWithKeyspaceID(
	ctx context.Context, re *require.Assertions,
	keyspaceID uint32, endpoints []string, opts ...opt.ClientOption,
) pd.Client {
	cli, err := pd.NewClientWithKeyspace(ctx,
		caller.TestComponent,
		keyspaceID, endpoints, pd.SecurityOption{}, opts...)
	re.NoError(err)
	return cli
}

// WaitForTSOServiceAvailable waits for the pd client being served by the tso server side
func WaitForTSOServiceAvailable(
	ctx context.Context, re *require.Assertions, client pd.Client,
) {
	testutil.Eventually(re, func() bool {
		_, _, err := client.GetTS(ctx)
		return err == nil
	})
}

// CheckMultiKeyspacesTSO checks the correctness of TSO for multiple keyspaces.
func CheckMultiKeyspacesTSO(
	ctx context.Context, re *require.Assertions,
	clients []pd.Client, parallelAct func(),
) {
	ctx, cancel := context.WithCancel(ctx)
	wg := sync.WaitGroup{}
	wg.Add(len(clients))

	for _, client := range clients {
		go func(cli pd.Client) {
			defer wg.Done()
			var ts, lastTS uint64
			for {
				select {
				case <-ctx.Done():
					// Make sure the lastTS is not empty
					re.NotEmpty(lastTS)
					return
				default:
				}
				physical, logical, err := cli.GetTS(ctx)
				// omit the error check since there are many kinds of errors
				if err != nil {
					continue
				}
				ts = tsoutil.ComposeTS(physical, logical)
				re.Less(lastTS, ts)
				lastTS = ts
			}
		}(client)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		parallelAct()
		cancel()
	}()

	wg.Wait()
}

// WaitForMultiKeyspacesTSOAvailable waits for the given keyspaces being served by the tso server side
func WaitForMultiKeyspacesTSOAvailable(
	ctx context.Context, re *require.Assertions,
	keyspaceIDs []uint32, backendEndpoints []string,
) []pd.Client {
	wg := sync.WaitGroup{}
	wg.Add(len(keyspaceIDs))

	clients := make([]pd.Client, 0, len(keyspaceIDs))
	for _, keyspaceID := range keyspaceIDs {
		cli := SetupClientWithKeyspaceID(ctx, re, keyspaceID, backendEndpoints, opt.WithForwardingOption(true))
		re.NotNil(cli)
		clients = append(clients, cli)

		go func() {
			defer wg.Done()
			testutil.Eventually(re, func() bool {
				_, _, err := cli.GetTS(ctx)
				return err == nil
			})
		}()
	}

	wg.Wait()
	return clients
}
