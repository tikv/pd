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

package realcluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/tikv/pd/client/http"
)

type rebootPDSuite struct {
	clusterSuite
}

func TestRebootPD(t *testing.T) {
	suite.Run(t, &rebootPDSuite{
		clusterSuite: clusterSuite{
			suiteName: "reboot_pd",
		},
	})
}

// https://github.com/tikv/pd/issues/6467
func (s *rebootPDSuite) TestReloadLabel() {
	re := s.Require()
	ctx := context.Background()

	pdHTTPCli := http.NewClient("pd-real-cluster-test", getPDEndpoints(re))
	defer pdHTTPCli.Close()
	resp, err := pdHTTPCli.GetStores(ctx)
	re.NoError(err)
	re.NotEmpty(resp.Stores)
	firstStore := resp.Stores[0]
	storeLabels := map[string]string{
		"zone": "zone1",
	}
	expectedLabels := make(map[string]string, len(firstStore.Store.Labels)+len(storeLabels))
	originalZone, hasOriginalZone := "", false
	for _, label := range firstStore.Store.Labels {
		expectedLabels[label.Key] = label.Value
		if label.Key == "zone" {
			originalZone, hasOriginalZone = label.Value, true
		}
	}
	for key, value := range storeLabels {
		expectedLabels[key] = value
	}
	// SetStoreLabels merges labels server-side. Do not echo existing labels
	// back in the request because "engine" is reserved for TiKV/TiFlash.
	re.NoError(pdHTTPCli.SetStoreLabels(ctx, firstStore.Store.ID, storeLabels))
	defer func() {
		// The restarted client is closed before this cleanup runs because defers
		// run in LIFO order, so use a fresh client to restore the store labels.
		cleanupCli := http.NewClient("pd-real-cluster-test", getPDEndpoints(re))
		defer cleanupCli.Close()
		if hasOriginalZone {
			re.NoError(cleanupCli.SetStoreLabels(ctx, firstStore.Store.ID, map[string]string{
				"zone": originalZone,
			}))
			return
		}
		re.NoError(cleanupCli.DeleteStoreLabel(ctx, firstStore.Store.ID, "zone"))
	}()

	checkLabelsAreEqual := func() {
		resp, err := pdHTTPCli.GetStore(ctx, uint64(firstStore.Store.ID))
		re.NoError(err)

		labelsMap := make(map[string]string)
		for _, label := range resp.Store.Labels {
			labelsMap[label.Key] = label.Value
		}

		for key, value := range expectedLabels {
			re.Equal(value, labelsMap[key])
		}
	}
	// Check the label is set
	checkLabelsAreEqual()
	// Restart to reload the label
	s.cluster.restart()
	pdHTTPCli = http.NewClient("pd-real-cluster-test", getPDEndpoints(re))
	defer pdHTTPCli.Close()
	checkLabelsAreEqual()
}
