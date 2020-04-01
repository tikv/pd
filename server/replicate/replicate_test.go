// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package replicate

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	pb "github.com/pingcap/kvproto/pkg/replicate_mode"
	"github.com/pingcap/pd/v4/pkg/mock/mockcluster"
	"github.com/pingcap/pd/v4/pkg/mock/mockid"
	"github.com/pingcap/pd/v4/pkg/mock/mockoption"
	"github.com/pingcap/pd/v4/pkg/typeutil"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/kv"
)

func TestReplicateMode(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testReplicateMode{})

type testReplicateMode struct{}

func (s *testReplicateMode) TestInitial(c *C) {
	store := core.NewStorage(kv.NewMemoryKV())
	id := mockid.NewIDAllocator()
	conf := config.ReplicateModeConfig{ReplicateMode: modeMajority}
	cluster := mockcluster.NewCluster(mockoption.NewScheduleOptions())
	rep, err := NewReplicateModeManager(conf, store, id, cluster)
	c.Assert(err, IsNil)
	c.Assert(rep.GetReplicateStatus(), DeepEquals, &pb.ReplicateStatus{Mode: pb.ReplicateStatus_MAJORITY})

	conf = config.ReplicateModeConfig{ReplicateMode: modeDRAutosync, DRAutoSync: config.DRAutoSyncReplicateConfig{
		LabelKey:         "dr-label",
		Primary:          "l1",
		DR:               "l2",
		PrimaryReplicas:  2,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
		WaitSyncTimeout:  typeutil.Duration{Duration: time.Minute},
	}}
	rep, err = NewReplicateModeManager(conf, store, id, cluster)
	c.Assert(err, IsNil)
	c.Assert(rep.GetReplicateStatus(), DeepEquals, &pb.ReplicateStatus{
		Mode: pb.ReplicateStatus_DR_AUTOSYNC,
		DrAutosync: &pb.DRAutoSync{
			LabelKey: "dr-label",
			State:    pb.DRAutoSync_SYNC,
		},
	})
}

func (s *testReplicateMode) TestStatus(c *C) {
	store := core.NewStorage(kv.NewMemoryKV())
	id := mockid.NewIDAllocator()
	conf := config.ReplicateModeConfig{ReplicateMode: modeDRAutosync, DRAutoSync: config.DRAutoSyncReplicateConfig{
		LabelKey:        "dr-label",
		WaitSyncTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(mockoption.NewScheduleOptions())
	rep, err := NewReplicateModeManager(conf, store, id, cluster)
	c.Assert(err, IsNil)
	c.Assert(rep.GetReplicateStatus(), DeepEquals, &pb.ReplicateStatus{
		Mode: pb.ReplicateStatus_DR_AUTOSYNC,
		DrAutosync: &pb.DRAutoSync{
			LabelKey: "dr-label",
			State:    pb.DRAutoSync_SYNC,
		},
	})

	err = rep.drSwitchToAsync()
	c.Assert(err, IsNil)
	c.Assert(rep.GetReplicateStatus(), DeepEquals, &pb.ReplicateStatus{
		Mode: pb.ReplicateStatus_DR_AUTOSYNC,
		DrAutosync: &pb.DRAutoSync{
			LabelKey: "dr-label",
			State:    pb.DRAutoSync_ASYNC,
		},
	})

	err = rep.drSwitchToSyncRecover()
	c.Assert(err, IsNil)
	recoverID := rep.drAutosync.RecoverID
	c.Assert(rep.GetReplicateStatus(), DeepEquals, &pb.ReplicateStatus{
		Mode: pb.ReplicateStatus_DR_AUTOSYNC,
		DrAutosync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSync_SYNC_RECOVER,
			RecoverId:           recoverID,
			WaitSyncTimeoutHint: 60,
		},
	})

	// test reload
	rep, err = NewReplicateModeManager(conf, store, id, cluster)
	c.Assert(err, IsNil)
	c.Assert(rep.drAutosync.State, Equals, drStateSyncRecover)

	err = rep.drSwitchToSync()
	c.Assert(err, IsNil)
	c.Assert(rep.GetReplicateStatus(), DeepEquals, &pb.ReplicateStatus{
		Mode: pb.ReplicateStatus_DR_AUTOSYNC,
		DrAutosync: &pb.DRAutoSync{
			LabelKey: "dr-label",
			State:    pb.DRAutoSync_SYNC,
		},
	})
}

func (s *testReplicateMode) TestStateSwitch(c *C) {
	store := core.NewStorage(kv.NewMemoryKV())
	id := mockid.NewIDAllocator()
	conf := config.ReplicateModeConfig{ReplicateMode: modeDRAutosync, DRAutoSync: config.DRAutoSyncReplicateConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		PrimaryReplicas:  2,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
		WaitSyncTimeout:  typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(mockoption.NewScheduleOptions())
	rep, err := NewReplicateModeManager(conf, store, id, cluster)
	c.Assert(err, IsNil)

	cluster.AddLabelsStore(1, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(3, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(4, 1, map[string]string{"zone": "zone2"})
	cluster.AddLabelsStore(5, 1, map[string]string{"zone": "zone2"})

	// initial state is sync
	c.Assert(rep.drGetState(), Equals, drStateSync)

	// sync -> async
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSync)
	s.setStoreState(cluster, 1, "down")
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSync)
	s.setStoreState(cluster, 2, "down")
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateAsync)
	rep.drSwitchToSync()
	s.setStoreState(cluster, 1, "up")
	s.setStoreState(cluster, 2, "up")
	s.setStoreState(cluster, 5, "down")
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateAsync)

	// async -> sync_recover
	s.setStoreState(cluster, 5, "up")
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSyncRecover)
	rep.drSwitchToAsync()
	s.setStoreState(cluster, 1, "down")
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSyncRecover)

	// sync_recover -> async
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSyncRecover)
	s.setStoreState(cluster, 4, "down")
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateAsync)

	// sync_recover -> sync
	rep.drSwitchToSyncRecover()
	s.setStoreState(cluster, 4, "up")
	rep.drAutosync.RecoverStartTime = time.Now().Add(-time.Hour)
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSync)
}

func (s *testReplicateMode) setStoreState(cluster *mockcluster.Cluster, id uint64, state string) {
	store := cluster.GetStore(id)
	if state == "down" {
		store.GetMeta().LastHeartbeat = time.Now().Add(-time.Minute * 10).UnixNano()
	} else if state == "up" {
		store.GetMeta().LastHeartbeat = time.Now().UnixNano()
	}
	cluster.PutStore(store)
}
