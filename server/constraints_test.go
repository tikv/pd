// Copyright 2016 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
)

var _ = Suite(&testConstraintSuite{})

type testConstraintSuite struct{}

func (s *testConstraintSuite) Test(c *C) {
	store := newStoreInfo(&metapb.Store{Id: 1})

	// Empty constraint matches any stores.
	cst := &Constraint{}
	c.Assert(cst.Match(store), IsTrue)
	cst.Labels = make(map[string]string)
	c.Assert(cst.Match(store), IsTrue)

	store.Labels = append(store.Labels,
		&metapb.StoreLabel{Key: "zone", Value: "us-west"},
		&metapb.StoreLabel{Key: "disk", Value: "ssd"},
	)

	cst.Labels["zone"] = "us-east"
	c.Assert(cst.Match(store), IsFalse)
	cst.Labels["zone"] = "us-west"
	c.Assert(cst.Match(store), IsTrue)
	cst.Labels["disk"] = "ssd"
	c.Assert(cst.Match(store), IsTrue)
	cst.Labels["disk"] = "hdd"
	c.Assert(cst.Match(store), IsFalse)
}

var _ = Suite(&testConstraintsSuite{})

type testConstraintsSuite struct{}

func (s *testConstraintsSuite) TestMaxReplicas(c *C) {
	var constraints []*Constraint

	// constraint with no replicas, error.
	_, err := newConstraints(3, []*Constraint{{}})
	c.Assert(err, NotNil)

	// max replicas 3 > sum replicas 2, OK.
	constraints = append(constraints, &Constraint{
		Labels:   map[string]string{"test": "1"},
		Replicas: 2,
	})
	_, err = newConstraints(3, constraints)
	c.Assert(err, IsNil)

	// max replicas 3 = sum replicas 3, OK.
	constraints = append(constraints, &Constraint{
		Labels:   map[string]string{"test": "2"},
		Replicas: 1,
	})
	_, err = newConstraints(3, constraints)
	c.Assert(err, IsNil)

	// max replicas 3 < sum replicas 4, error.
	constraints = append(constraints, &Constraint{
		Labels:   map[string]string{"test": "3"},
		Replicas: 1,
	})
	_, err = newConstraints(3, constraints)
	c.Assert(err, NotNil)

	// max replicas 4 is even, error.
	_, err = newConstraints(4, constraints)
	c.Assert(err, NotNil)

	// max replicas 5 > sum replicas 4, OK.
	_, err = newConstraints(5, constraints)
	c.Assert(err, IsNil)
}

func (s *testConstraintsSuite) TestExclusive(c *C) {
	// "zone"s and "disk"s are both not the same, OK.
	constraints := []*Constraint{
		{
			Labels:   map[string]string{"zone": "us-west", "disk": "ssd"},
			Replicas: 2,
		},
		{
			Labels:   map[string]string{"zone": "us-east", "disk": "hdd"},
			Replicas: 1,
		},
	}
	_, err := newConstraints(3, constraints)
	c.Assert(err, IsNil)

	// "zone"s are the same, but "disk"s are not, OK.
	constraints[0].Labels = map[string]string{"zone": "us-west", "disk": "ssd"}
	constraints[1].Labels = map[string]string{"zone": "us-west", "disk": "hdd"}
	_, err = newConstraints(3, constraints)
	c.Assert(err, IsNil)

	// All labels are the same, error.
	constraints[0].Labels = constraints[1].Labels
	_, err = newConstraints(3, constraints)
	c.Assert(err, NotNil)

	// "zone"s are the same.
	constraints[0].Labels = map[string]string{"zone": "us-east"}
	constraints[1].Labels = map[string]string{"zone": "us-east", "disk": "hdd"}
	_, err = newConstraints(3, constraints)
	c.Assert(err, NotNil)

	// "disk"s are the same.
	constraints[0].Labels = map[string]string{"zone": "us-east", "disk": "hdd"}
	constraints[1].Labels = map[string]string{"test": "test", "disk": "hdd"}
	_, err = newConstraints(3, constraints)
	c.Assert(err, NotNil)
}

func (s *testConstraintsSuite) TestConstraints(c *C) {
	constraints := []*Constraint{
		{
			Labels:   map[string]string{"zone": "us-west", "disk": "ssd"},
			Replicas: 2,
		},
		{
			Labels:   map[string]string{"zone": "us-east", "disk": "hdd"},
			Replicas: 1,
		},
	}
	cs, _ := newConstraints(3, constraints)

	var stores []*storeInfo
	result := cs.Match(stores)
	c.Assert(result.stores, HasLen, 0)
	c.Assert(result.constraints[0].stores, IsNil)
	c.Assert(result.constraints[1].stores, IsNil)

	// Create 5 stores with no labels.
	stores = append(stores, newStoreInfo(&metapb.Store{Id: 0}))
	stores = append(stores, newStoreInfo(&metapb.Store{Id: 1}))
	stores = append(stores, newStoreInfo(&metapb.Store{Id: 2}))
	stores = append(stores, newStoreInfo(&metapb.Store{Id: 3}))
	stores = append(stores, newStoreInfo(&metapb.Store{Id: 4}))

	result = cs.Match(stores)
	c.Assert(result.stores, HasLen, 0)
	c.Assert(result.constraints[0].stores, IsNil)
	c.Assert(result.constraints[1].stores, IsNil)

	// Store 0 matches constraint 0.
	// Store 1 matches constraint 1.
	stores[0].Labels = []*metapb.StoreLabel{
		{Key: "zone", Value: "us-west"},
		{Key: "disk", Value: "ssd"},
		{Key: "test", Value: "test"},
	}
	stores[1].Labels = []*metapb.StoreLabel{
		{Key: "zone", Value: "us-east"},
		{Key: "disk", Value: "hdd"},
	}
	stores[3].Labels = []*metapb.StoreLabel{
		{Key: "zone", Value: "us-west"},
		{Key: "disk", Value: "hdd"},
		{Key: "example", Value: "example"},
	}
	stores[4].Labels = []*metapb.StoreLabel{
		{Key: "zone", Value: "us-east"},
		{Key: "disk", Value: "ssd"},
	}

	result = cs.Match(stores)
	c.Assert(result.stores, HasLen, 2)
	c.Assert(result.stores[0], DeepEquals, cs.Constraints[0])
	c.Assert(result.stores[1], DeepEquals, cs.Constraints[1])
	c.Assert(result.constraints[0].stores, DeepEquals, stores[0:1])
	c.Assert(result.constraints[1].stores, DeepEquals, stores[1:2])

	// Store 0,1 can match constraint 0.
	// Store 3,4 can match constraint 1 (but store 4 is redundant).
	stores[0].Labels = []*metapb.StoreLabel{
		{Key: "zone", Value: "us-west"},
		{Key: "disk", Value: "ssd"},
		{Key: "test", Value: "test"},
	}
	stores[1].Labels = []*metapb.StoreLabel{
		{Key: "zone", Value: "us-west"},
		{Key: "disk", Value: "ssd"},
	}
	stores[3].Labels = []*metapb.StoreLabel{
		{Key: "zone", Value: "us-east"},
		{Key: "disk", Value: "hdd"},
		{Key: "example", Value: "example"},
	}
	stores[4].Labels = []*metapb.StoreLabel{
		{Key: "zone", Value: "us-east"},
		{Key: "disk", Value: "hdd"},
	}

	result = cs.Match(stores)
	c.Assert(result.stores, HasLen, 3)
	c.Assert(result.stores[0], DeepEquals, cs.Constraints[0])
	c.Assert(result.stores[1], DeepEquals, cs.Constraints[0])
	c.Assert(result.stores[3], DeepEquals, cs.Constraints[1])
	c.Assert(result.constraints[0].stores, DeepEquals, stores[0:2])
	c.Assert(result.constraints[1].stores, DeepEquals, stores[3:4])

	// Store 0,1,2 can match constraint 0 (but store 2 is redundant).
	// Store 3,4 can match constraint 1 (but store 4 is redundant).
	stores[0].Labels = []*metapb.StoreLabel{
		{Key: "zone", Value: "us-west"},
		{Key: "disk", Value: "ssd"},
	}
	stores[1].Labels = []*metapb.StoreLabel{
		{Key: "zone", Value: "us-west"},
		{Key: "disk", Value: "ssd"},
		{Key: "abc", Value: "cba"},
	}
	stores[2].Labels = []*metapb.StoreLabel{
		{Key: "zone", Value: "us-west"},
		{Key: "disk", Value: "ssd"},
	}
	stores[3].Labels = []*metapb.StoreLabel{
		{Key: "zone", Value: "us-east"},
		{Key: "disk", Value: "hdd"},
		{Key: "test", Value: "test"},
	}
	stores[4].Labels = []*metapb.StoreLabel{
		{Key: "zone", Value: "us-east"},
		{Key: "disk", Value: "hdd"},
		{Key: "example", Value: "example"},
	}

	result = cs.Match(stores)
	c.Assert(result.stores, HasLen, 3)
	c.Assert(result.stores[0], DeepEquals, cs.Constraints[0])
	c.Assert(result.stores[1], DeepEquals, cs.Constraints[0])
	c.Assert(result.stores[3], DeepEquals, cs.Constraints[1])
	c.Assert(result.constraints[0].stores, DeepEquals, stores[0:2])
	c.Assert(result.constraints[1].stores, DeepEquals, stores[3:4])
}
