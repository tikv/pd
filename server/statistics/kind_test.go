// Copyright 2021 TiKV Project Authors.
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

package statistics

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server/core"
)

var _ = Suite(&testRegionInfoSuite{})

type testRegionInfoSuite struct{}

func (s *testRegionInfoSuite) TestGetLoads(c *C) {
	queryStats := &pdpb.QueryStats{
		Get:         5,
		Coprocessor: 6,
		Scan:        7,
		Put:         8,
		Delete:      9,
		DeleteRange: 10,
	}
	regionA := core.NewRegionInfo(&metapb.Region{Id: 100, Peers: []*metapb.Peer{}}, nil,
		core.SetReadBytes(1),
		core.SetReadKeys(2),
		core.SetWrittenBytes(3),
		core.SetWrittenKeys(4),
		core.SetQueryStats(queryStats))
	loads := regionA.GetLoads()
	c.Assert(loads, HasLen, int(RegionStatCount))
	c.Assert(float64(regionA.GetBytesRead()), Equals, loads[RegionReadBytes])
	c.Assert(float64(regionA.GetKeysRead()), Equals, loads[RegionReadKeys])
	c.Assert(float64(regionA.GetReadQueryNum()), Equals, loads[RegionReadQuery])
	c.Assert(float64(regionA.GetBytesWritten()), Equals, loads[RegionWriteBytes])
	c.Assert(float64(regionA.GetKeysWritten()), Equals, loads[RegionWriteKeys])
	c.Assert(float64(regionA.GetWriteQueryNum()), Equals, loads[RegionWriteQuery])

	loads = regionA.GetWriteLoads()
	c.Assert(loads, HasLen, int(RegionStatCount))
	c.Assert(0.0, Equals, loads[RegionReadBytes])
	c.Assert(0.0, Equals, loads[RegionReadKeys])
	c.Assert(0.0, Equals, loads[RegionReadQuery])
	c.Assert(float64(regionA.GetBytesWritten()), Equals, loads[RegionWriteBytes])
	c.Assert(float64(regionA.GetKeysWritten()), Equals, loads[RegionWriteKeys])
	c.Assert(float64(regionA.GetWriteQueryNum()), Equals, loads[RegionWriteQuery])
}
