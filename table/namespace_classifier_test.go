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

package table

import (
	"sort"
	"sync/atomic"

	"bytes"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testTableNamespaceSuite{})

type testTableNamespaceSuite struct {
}

type mockTableIDDecoderForTarget struct{}
type mockTableIDDecoderForGlobal struct{}
type mockTableIDDecoderForEdge struct{}
type mockTableIDDecoderForCrossTable struct{}

const (
	targetTableID = 12345
	targetStoreID = 54321
	globalTableID = 789
	globalStoreID = 987
)

var tableStartKey = []byte{'t', 0, 0, 0, '1', 0, 0, 0, 0}

func (d mockTableIDDecoderForTarget) DecodeTableID(key Key) int64 {
	return targetTableID
}

func (d mockTableIDDecoderForGlobal) DecodeTableID(key Key) int64 {
	return globalTableID
}

func (d mockTableIDDecoderForEdge) DecodeTableID(key Key) int64 {
	if string(key) == "startKey" || string(key) == "endKey" {
		return 0
	}
	return targetTableID
}

func (d mockTableIDDecoderForCrossTable) DecodeTableID(key Key) int64 {
	if string(key) == "startKey" {
		return targetTableID
	} else if string(key) == "endKey" {
		return targetTableID + 1
	} else if bytes.Equal(key, tableStartKey) {
		return targetTableID + 1
	}
	return targetTableID
}

// mockIDAllocator mocks IDAllocator and it is only used for test.
type mockIDAllocator struct {
	base uint64
}

func newMockIDAllocator() *mockIDAllocator {
	return &mockIDAllocator{base: 0}
}

func (alloc *mockIDAllocator) Alloc() (uint64, error) {
	return atomic.AddUint64(&alloc.base, 1), nil
}

func (s *testTableNamespaceSuite) newClassifier(c *C, decoder IDDecoder) *tableNamespaceClassifier {
	kv := core.NewKV(core.NewMemoryKV())
	classifier, err := NewTableNamespaceClassifier(kv, &mockIDAllocator{})
	c.Assert(err, IsNil)
	tableClassifier := classifier.(*tableNamespaceClassifier)
	tableClassifier.tableIDDecoder = decoder
	testNamespace1 := Namespace{
		ID:   1,
		Name: "test1",
		TableIDs: map[int64]bool{
			targetTableID: true,
		},
		StoreIDs: map[uint64]bool{
			targetStoreID: true,
		},
	}

	testNamespace2 := Namespace{
		ID:   2,
		Name: "test2",
		TableIDs: map[int64]bool{
			targetTableID + 1: true,
		},
		StoreIDs: map[uint64]bool{
			targetStoreID + 1: true,
		},
	}

	tableClassifier.nsInfo.setNamespace(&testNamespace1)
	tableClassifier.nsInfo.setNamespace(&testNamespace2)
	return tableClassifier
}

func (s *testTableNamespaceSuite) TestTableNameSpaceGetAllNamespace(c *C) {
	classifier := s.newClassifier(c, mockTableIDDecoderForTarget{})
	ns := classifier.GetAllNamespaces()
	sort.Strings(ns)
	c.Assert(ns, DeepEquals, []string{"test1", "test2"})
}

func (s *testTableNamespaceSuite) TestTableNameSpaceGetStoreNamespace(c *C) {
	classifier := s.newClassifier(c, mockTableIDDecoderForTarget{})

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
	classifier := s.newClassifier(c, mockTableIDDecoderForTarget{})
	regionInfo := core.NewRegionInfo(&metapb.Region{}, &metapb.Peer{})
	c.Assert(classifier.GetRegionNamespace(regionInfo), Equals, "test1")

	// Test region namespace when tableIDDecoder doesn't return the region's tableId
	classifier = s.newClassifier(c, mockTableIDDecoderForGlobal{})
	regionInfo = core.NewRegionInfo(&metapb.Region{}, &metapb.Peer{})
	c.Assert(classifier.GetRegionNamespace(regionInfo), Equals, "global")
}

func (s *testTableNamespaceSuite) TestNamespaceOperation(c *C) {
	kv := core.NewKV(core.NewMemoryKV())
	classifier, err := NewTableNamespaceClassifier(kv, &mockIDAllocator{})
	c.Assert(err, IsNil)
	tableClassifier := classifier.(*tableNamespaceClassifier)
	tableClassifier.tableIDDecoder = mockTableIDDecoderForGlobal{}
	nsInfo := tableClassifier.nsInfo

	err = tableClassifier.CreateNamespace("(invalid_name")
	c.Assert(err, NotNil)

	err = tableClassifier.CreateNamespace("test1")
	c.Assert(err, IsNil)

	namespaces := tableClassifier.GetNamespaces()
	c.Assert(len(namespaces), Equals, 1)
	c.Assert(namespaces[0].Name, Equals, "test1")

	// Add the same Name
	err = tableClassifier.CreateNamespace("test1")
	c.Assert(err, NotNil)

	tableClassifier.CreateNamespace("test2")

	// Add tableID
	err = tableClassifier.AddNamespaceTableID("test1", 1)
	namespaces = tableClassifier.GetNamespaces()
	c.Assert(err, IsNil)
	c.Assert(nsInfo.IsTableIDExist(1), IsTrue)

	// Add storeID
	err = tableClassifier.AddNamespaceStoreID("test1", 456)
	namespaces = tableClassifier.GetNamespaces()
	c.Assert(err, IsNil)
	c.Assert(nsInfo.IsStoreIDExist(456), IsTrue)

	// Ensure that duplicate tableID cannot exist in one namespace
	err = tableClassifier.AddNamespaceTableID("test1", 1)
	c.Assert(err, NotNil)

	// Ensure that duplicate tableID cannot exist across namespaces
	err = tableClassifier.AddNamespaceTableID("test2", 1)
	c.Assert(err, NotNil)

	// Ensure that duplicate storeID cannot exist in one namespace
	err = tableClassifier.AddNamespaceStoreID("test1", 456)
	c.Assert(err, NotNil)

	// Ensure that duplicate storeID cannot exist across namespaces
	err = tableClassifier.AddNamespaceStoreID("test2", 456)
	c.Assert(err, NotNil)

	// Add tableID to a namespace that doesn't exist
	err = tableClassifier.AddNamespaceTableID("test_not_exist", 2)
	c.Assert(err, NotNil)
}

func (s *testTableNamespaceSuite) TestClassifierWithInfiniteEdge(c *C) {
	// mock the start edge
	classifier := s.newClassifier(c, mockTableIDDecoderForEdge{})
	regionInfo := core.NewRegionInfo(&metapb.Region{
		StartKey: []byte("startKey"),
	}, &metapb.Peer{})
	ns := classifier.GetRegionNamespace(regionInfo)
	c.Assert(ns, Equals, "test1")

	// mock the end edge
	classifier = s.newClassifier(c, mockTableIDDecoderForEdge{})
	regionInfo = core.NewRegionInfo(&metapb.Region{
		EndKey: []byte("endKey"),
	}, &metapb.Peer{})
	ns = classifier.GetRegionNamespace(regionInfo)
	c.Assert(ns, Equals, "test1")

	// mock the region ("", ""), should return global
	classifier = s.newClassifier(c, mockTableIDDecoderForEdge{})
	regionInfo = core.NewRegionInfo(&metapb.Region{
		StartKey: []byte("startKey"),
		EndKey:   []byte("endKey"),
	}, &metapb.Peer{})
	ns = classifier.GetRegionNamespace(regionInfo)
	c.Assert(ns, Equals, "global")
}

func (s *testTableNamespaceSuite) TestClassifierWithCrossTable(c *C) {
	// mock the cross table
	classifier := s.newClassifier(c, mockTableIDDecoderForCrossTable{})
	regionInfo := core.NewRegionInfo(&metapb.Region{
		StartKey: []byte("startKey"),
		EndKey:   []byte("endKey"),
	}, &metapb.Peer{})
	ns := classifier.GetRegionNamespace(regionInfo)
	c.Assert(ns, Equals, "global")
}

func (s *testTableNamespaceSuite) TestClassifierWithTableSplit(c *C) {
	// mock the cross table
	classifier := s.newClassifier(c, mockTableIDDecoderForCrossTable{})
	regionInfo := core.NewRegionInfo(&metapb.Region{
		StartKey: []byte("startKey"),
		EndKey:   tableStartKey,
	}, &metapb.Peer{})
	ns := classifier.GetRegionNamespace(regionInfo)
	c.Assert(ns, Equals, "test1")
}
