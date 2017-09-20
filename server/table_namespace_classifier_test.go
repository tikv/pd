// Copyright 2017 PingCAP, Inc.
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

package server

import (
	"sort"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testTableNamespaceSuite{})

type testTableNamespaceSuite struct {
	namespaces *namespacesInfo
}

type mockTableIDDecoderForTarget struct{}
type mockTableIDDecoderForGlobal struct{}

const (
	targetTableID = 12345
	targetStoreID = 54321
	globalTableID = 789
	globalStoreID = 987
)

func (d mockTableIDDecoderForTarget) DecodeTableID(key core.Key) int64 {
	return targetTableID
}

func (d mockTableIDDecoderForGlobal) DecodeTableID(key core.Key) int64 {
	return globalTableID
}

func (s *testTableNamespaceSuite) SetUpSuite(c *C) {
	testNamespace1 := Namespace{
		ID:   1,
		Name: "test1",
		TableIDs: map[int64]int{
			targetTableID: 1,
		},
		StoreIDs: map[uint64]int{
			targetStoreID: 1,
		},
	}

	testNamespace2 := Namespace{
		ID:   2,
		Name: "test2",
		TableIDs: map[int64]int{
			targetTableID + 1: 1,
		},
		StoreIDs: map[uint64]int{
			targetStoreID + 1: 1,
		},
	}

	namespaces := newNamespacesInfo()
	namespaces.setNamespace(&testNamespace1)
	namespaces.setNamespace(&testNamespace2)
	s.namespaces = namespaces
}

func (s *testTableNamespaceSuite) TestTableNameSpaceGetAllNamespace(c *C) {

	classifier := newTableNamespaceClassifier(s.namespaces, mockTableIDDecoderForTarget{})
	ns := classifier.GetAllNamespaces()
	sort.Strings(ns)
	c.Assert(ns, DeepEquals, []string{"test1", "test2"})
}

func (s *testTableNamespaceSuite) TestTableNameSpaceGetStoreNamespace(c *C) {

	classifier := newTableNamespaceClassifier(s.namespaces, mockTableIDDecoderForTarget{})

	// Test store namespace
	meatapdStore := metapb.Store{Id: targetStoreID}
	storeInfo := core.NewStoreInfo(&meatapdStore)
	c.Assert(classifier.GetStoreNamespace(storeInfo), Equals, "test1")

	meatapdStore = metapb.Store{Id: globalStoreID}
	storeInfo = core.NewStoreInfo(&meatapdStore)
	c.Assert(classifier.GetStoreNamespace(storeInfo), Equals, "global")
}

func (s *testTableNamespaceSuite) TestTableNameSpaceGetRegionNamespace(c *C) {
	// Test region namespace when tableIDDecoder returns the region's tableId
	classifier := newTableNamespaceClassifier(s.namespaces, mockTableIDDecoderForTarget{})
	regionInfo := core.NewRegionInfo(&metapb.Region{}, &metapb.Peer{})
	c.Assert(classifier.GetRegionNamespace(regionInfo), Equals, "test1")

	// Test region namespace when tableIDDecoder doesn't return the region's tableId
	classifier = newTableNamespaceClassifier(s.namespaces, mockTableIDDecoderForGlobal{})
	regionInfo = core.NewRegionInfo(&metapb.Region{}, &metapb.Peer{})
	c.Assert(classifier.GetRegionNamespace(regionInfo), Equals, "global")

}
