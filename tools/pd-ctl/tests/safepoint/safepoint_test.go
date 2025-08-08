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

package safepoint_test

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/server/api"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

func TestSafepoint(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := pdTests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer tc.Destroy()
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	pdAddr := tc.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	// add some gc_safepoint to the server
	now := time.Now().Truncate(time.Second)
	list := &api.ListServiceGCSafepoint{
		ServiceGCSafepoints: []*endpoint.ServiceSafePoint{
			{
				ServiceID:  "AAA",
				ExpiredAt:  now.Unix() + 10,
				SafePoint:  10,
				KeyspaceID: constant.NullKeyspaceID,
			},
			{
				ServiceID:  "BBB",
				ExpiredAt:  now.Unix() + 10,
				SafePoint:  20,
				KeyspaceID: constant.NullKeyspaceID,
			},
			{
				ServiceID:  "CCC",
				ExpiredAt:  now.Unix() + 10,
				SafePoint:  30,
				KeyspaceID: constant.NullKeyspaceID,
			},
			{
				ServiceID:  "gc_worker",
				ExpiredAt:  math.MaxInt64,
				SafePoint:  1,
				KeyspaceID: constant.NullKeyspaceID,
			},
		},
		GCSafePoint:           1,
		MinServiceGcSafepoint: 1,
	}

	gcStateManager := leaderServer.GetServer().GetGCStateManager()
	// Skip writing "gc_worker.
	for _, ssp := range list.ServiceGCSafepoints[:3] {
		_, _, err = gcStateManager.CompatibleUpdateServiceGCSafePoint(constant.NullKeyspaceID, ssp.ServiceID, ssp.SafePoint, ssp.ExpiredAt-now.Unix(), now)
		re.NoError(err)
	}
	_, err = gcStateManager.AdvanceTxnSafePoint(constant.NullKeyspaceID, 1, now)
	re.NoError(err)
	_, _, err = gcStateManager.AdvanceGCSafePoint(constant.NullKeyspaceID, 1)
	re.NoError(err)

	// get the safepoints
	args := []string{"-u", pdAddr, "service-gc-safepoint"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)

	// create an container to hold the received values
	var l api.ListServiceGCSafepoint
	re.NoError(json.Unmarshal(output, &l))

	// test if the points are what we expected
	re.Equal(uint64(1), l.GCSafePoint)
	re.Equal(uint64(1), l.MinServiceGcSafepoint)
	re.Len(l.ServiceGCSafepoints, 4)

	// sort the gc safepoints based on order of ServiceID
	sort.Slice(l.ServiceGCSafepoints, func(i, j int) bool {
		return l.ServiceGCSafepoints[i].ServiceID < l.ServiceGCSafepoints[j].ServiceID
	})

	for i, val := range l.ServiceGCSafepoints {
		re.Equal(list.ServiceGCSafepoints[i].ServiceID, val.ServiceID)
		re.Equal(list.ServiceGCSafepoints[i].SafePoint, val.SafePoint)
	}

	// delete the safepoints
	for i := range 3 {
		args = []string{"-u", pdAddr, "service-gc-safepoint", "delete", list.ServiceGCSafepoints[i].ServiceID}
		output, err = tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		var msg string
		re.NoError(json.Unmarshal(output, &msg))
		re.Equal("Delete service GC safepoint successfully.", msg)
	}

	// do a second round of get safepoints to ensure that the safe points are indeed deleted
	args = []string{"-u", pdAddr, "service-gc-safepoint"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)

	// "gc_worker" will still exist in the result set as it's pseudo.
	var ll api.ListServiceGCSafepoint
	re.NoError(json.Unmarshal(output, &ll))

	re.Equal(uint64(1), ll.GCSafePoint)
	re.Equal(uint64(1), ll.MinServiceGcSafepoint)
	re.Len(ll.ServiceGCSafepoints, 1)
	re.Equal("gc_worker", ll.ServiceGCSafepoints[0].ServiceID)

	// try delete the "gc_worker"
	args = []string{"-u", pdAddr, "service-gc-safepoint", "delete", "gc_worker"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)

	// This should be rejected previously, but as GC barrier became the replacement of services safe points and we no
	// longer require the service safe point of "gc_worker", the deletion is now still allowed.
	var msg string
	re.NoError(json.Unmarshal(output, &msg))
	re.Equal("Delete service GC safepoint successfully.", msg)

	// "gc_worker" still exist after the deletion as it's pseudo.
	args = []string{"-u", pdAddr, "service-gc-safepoint"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &ll))

	re.Equal(uint64(1), ll.GCSafePoint)
	re.Equal(uint64(1), ll.MinServiceGcSafepoint)
	re.Len(ll.ServiceGCSafepoints, 1)
	re.Equal("gc_worker", ll.ServiceGCSafepoints[0].ServiceID)

	// try delete a non-exist safepoint, should return normally
	args = []string{"-u", pdAddr, "service-gc-safepoint", "delete", "non_exist"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &msg))
	re.Equal("Delete service GC safepoint successfully.", msg)
}
