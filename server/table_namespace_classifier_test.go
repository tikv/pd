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

func (s *testTableNamespaceSuite) newClassifier(c *C, decoder core.TableIDDecoder) *tableNamespaceClassifier {
	kv := core.NewKV(core.NewMemoryKV())
	classifier, err := newTableNamespaceClassifier(decoder, kv, &mockIDAllocator{})
	c.Assert(err, IsNil)
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

	classifier.nsInfo.setNamespace(&testNamespace1)
	classifier.nsInfo.setNamespace(&testNamespace2)
	return classifier
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
	classifier, err := newTableNamespaceClassifier(mockTableIDDecoderForGlobal{}, kv, &mockIDAllocator{})
	c.Assert(err, IsNil)
	nsInfo := classifier.nsInfo

	err = classifier.CreateNamespace("(invalid_name")
	c.Assert(err, NotNil)

	err = classifier.CreateNamespace("test1")
	c.Assert(err, IsNil)

	namespaces := classifier.GetNamespaces()
	c.Assert(len(namespaces), Equals, 1)
	c.Assert(namespaces[0].Name, Equals, "test1")

	// Add the same Name
	err = classifier.CreateNamespace("test1")
	c.Assert(err, NotNil)

	classifier.CreateNamespace("test2")

	// Add tableID
	err = classifier.AddNamespaceTableID("test1", 1)
	namespaces = classifier.GetNamespaces()
	c.Assert(err, IsNil)
	c.Assert(nsInfo.IsTableIDExist(1), IsTrue)

	// Add storeID
	err = classifier.AddNamespaceStoreID("test1", 456)
	namespaces = classifier.GetNamespaces()
	c.Assert(err, IsNil)
	c.Assert(nsInfo.IsStoreIDExist(456), IsTrue)

	// Ensure that duplicate tableID cannot exist in one namespace
	err = classifier.AddNamespaceTableID("test1", 1)
	c.Assert(err, NotNil)

	// Ensure that duplicate tableID cannot exist across namespaces
	err = classifier.AddNamespaceTableID("test2", 1)
	c.Assert(err, NotNil)

	// Ensure that duplicate storeID cannot exist in one namespace
	err = classifier.AddNamespaceStoreID("test1", 456)
	c.Assert(err, NotNil)

	// Ensure that duplicate storeID cannot exist across namespaces
	err = classifier.AddNamespaceStoreID("test2", 456)
	c.Assert(err, NotNil)

	// Add tableID to a namespace that doesn't exist
	err = classifier.AddNamespaceTableID("test_not_exist", 2)
	c.Assert(err, NotNil)
}
