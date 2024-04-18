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
	"os/exec"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
)

func restartTiUP() {
	log.Info("start to restart TiUP")
	cmd := exec.Command("make", "deploy")
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
	log.Info("TiUP restart success")
}

// https://github.com/tikv/pd/issues/6467
func TestReloadLabel(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	resp, _ := pdHTTPCli.GetStores(ctx)
	setStore := resp.Stores[0]
	// TiFlash labels will be ["engine": "tiflash"]
	storeLabel := map[string]string{
		"zone": "zone1",
	}
	for _, label := range setStore.Store.Labels {
		storeLabel[label.Key] = label.Value
	}
	err := pdHTTPCli.SetStoreLabels(ctx, setStore.Store.ID, storeLabel)
	re.NoError(err)

	resp, err = pdHTTPCli.GetStores(ctx)
	re.NoError(err)
	for _, store := range resp.Stores {
		if store.Store.ID == setStore.Store.ID {
			for _, label := range store.Store.Labels {
				re.Equal(label.Value, storeLabel[label.Key])
			}
		}
	}

	restartTiUP()

	resp, err = pdHTTPCli.GetStores(ctx)
	re.NoError(err)
	for _, store := range resp.Stores {
		if store.Store.ID == setStore.Store.ID {
			for _, label := range store.Store.Labels {
				re.Equal(label.Value, storeLabel[label.Key])
			}
		}
	}
}
