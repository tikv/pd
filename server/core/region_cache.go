// Copyright 2020 PingCAP, Inc.
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

package core

import (
	"bytes"
	"time"
)

type regionNode struct {
	region    *RegionInfo
	timestamp uint64
	count     int
	available bool
}

type regionQueueNode struct {
	rNode *regionNode
	pre   *regionQueueNode
	next  *regionQueueNode
}

type regionQueue struct {
	start *regionQueueNode
	end   *regionQueueNode
	len   int
	nodes map[uint64]*regionQueueNode
}

func newRegionQueue() *regionQueue {
	return &regionQueue{
		start: nil,
		end:   nil,
		len:   0,
		nodes: make(map[uint64]*regionQueueNode),
	}
}

// getNode gets the regionQueueNode which has the region with regionID.
func (queue *regionQueue) getNode(regionID uint64) *regionQueueNode {
	if node, ok := queue.nodes[regionID]; ok {
		return node
	}
	return nil
}

// getRegion gets a RegionInfo from regionCache.
func (queue *regionQueue) getRegion(regionID uint64) *RegionInfo {
	if node, ok := queue.nodes[regionID]; ok {
		return node.rNode.region
	}
	return nil
}

// getRegions gets all RegionInfo from regionQueue.
func (queue *regionQueue) getRegions() []*RegionInfo {
	var regions []*RegionInfo
	for _, node := range queue.nodes {
		regions = append(regions, node.rNode.region)
	}
	return regions
}

// push adds region to the end of regionQueue.
func (queue *regionQueue) push(rNode *regionNode) *regionQueueNode {
	if rNode == nil {
		return nil
	}
	if queue.start == nil {
		queue.start = &regionQueueNode{
			rNode: rNode,
			pre:   nil,
			next:  nil,
		}
		queue.len++
		queue.end = queue.start
		queue.nodes[rNode.region.GetID()] = queue.start
		return queue.start
	}
	queue.end.next = &regionQueueNode{
		rNode: rNode,
		pre:   queue.end,
		next:  nil,
	}
	queue.len++
	queue.end = queue.end.next
	queue.nodes[rNode.region.GetID()] = queue.end
	return queue.end
}

// update updates the RegionInfo or add it into regionQueue.
func (queue *regionQueue) update(rNode *regionNode) *regionQueueNode {
	if rNode == nil {
		return nil
	}
	if node, ok := queue.nodes[rNode.region.GetID()]; ok {
		node.rNode = rNode
		return node
	}
	return queue.push(rNode)
}

// pop deletes the first region in regionQueue and return it.
func (queue *regionQueue) pop() *RegionInfo {
	if queue.start == nil {
		return nil
	}
	region := queue.start.rNode.region
	queue.start = queue.start.next
	if queue.start != nil {
		queue.start.pre = nil
	}
	queue.len--
	delete(queue.nodes, region.GetID())
	return region
}

// remove deletes the regionQueueNode in regionQueue.
func (queue *regionQueue) remove(regionID uint64) bool {
	if queue == nil {
		return false
	}
	node := queue.getNode(regionID)
	if node == nil {
		return false
	}
	if node.pre != nil {
		node.pre.next = node.next
	} else {
		queue.start = node.next
	}
	if node.next != nil {
		node.next.pre = node.pre
	} else {
		queue.end = node.pre
	}
	queue.len--
	delete(queue.nodes, regionID)
	return true
}

type regionCache struct {
	regions       *regionQueue
	leaders       map[uint64]*regionQueue
	followers     map[uint64]*regionQueue
	learners      map[uint64]*regionQueue
	pendingPeers  map[uint64]*regionQueue
	lastTimestamp uint64
}

func newRegionCache() *regionCache {
	return &regionCache{
		regions:       newRegionQueue(),
		leaders:       make(map[uint64]*regionQueue),
		followers:     make(map[uint64]*regionQueue),
		learners:      make(map[uint64]*regionQueue),
		pendingPeers:  make(map[uint64]*regionQueue),
		lastTimestamp: uint64(time.Now().Unix()),
	}
}

func (cache *regionCache) length() int {
	return cache.regions.len
}

// getRegions get a RegionInfo from regionCache.
func (cache *regionCache) getRegion(regionID uint64) *RegionInfo {
	return cache.regions.getRegion(regionID)
}

// getRegions gets all RegionInfo from regionCache.
func (cache *regionCache) getRegions() []*RegionInfo {
	return cache.regions.getRegions()
}

// update updates the RegionInfo or add it into regionCache.
func (cache *regionCache) update(region *RegionInfo) {
	if region == nil {
		return
	}
	var newRegionNode *regionNode
	if node := cache.regions.getNode(region.GetID()); node != nil {
		cache.removeSubQueue(node.rNode.region)
		newRegionNode = node.rNode
		newRegionNode.region = region
	} else {
		newRegionNode = &regionNode{
			region:    region,
			timestamp: uint64(time.Now().Unix()),
			count:     0,
			available: true,
		}
		cache.regions.push(newRegionNode)
	}

	// Add to leaders and followers.
	for _, peer := range region.GetVoters() {
		storeID := peer.GetStoreId()
		if peer.GetId() == region.leader.GetId() {
			// Add leader peer to leaders.
			store, ok := cache.leaders[storeID]
			if !ok {
				store = newRegionQueue()
				cache.leaders[storeID] = store
			}
			store.update(newRegionNode)
		} else {
			// Add follower peer to followers.
			store, ok := cache.followers[storeID]
			if !ok {
				store = newRegionQueue()
				cache.followers[storeID] = store
			}
			store.update(newRegionNode)
		}
	}

	// Add to learners.
	for _, peer := range region.GetLearners() {
		storeID := peer.GetStoreId()
		store, ok := cache.learners[storeID]
		if !ok {
			store = newRegionQueue()
			cache.learners[storeID] = store
		}
		store.update(newRegionNode)
	}

	for _, peer := range region.pendingPeers {
		storeID := peer.GetStoreId()
		store, ok := cache.pendingPeers[storeID]
		if !ok {
			store = newRegionQueue()
			cache.pendingPeers[storeID] = store
		}
		store.update(newRegionNode)
	}
}

// pop deletes the oldest region and return it.
func (cache *regionCache) pop() *RegionInfo {
	if region := cache.regions.pop(); region != nil {
		cache.removeSubQueue(region)
		return region
	}
	return nil
}

// remove deletes the region in regionCache.
func (cache *regionCache) remove(regionID uint64) {
	if region := cache.getRegion(regionID); region != nil {
		cache.removeSubQueue(region)
		cache.regions.remove(regionID)
	}
}

// removeSubQueue deletes the region in all classified queues.
func (cache *regionCache) removeSubQueue(region *RegionInfo) {
	for _, peer := range region.meta.Peers {
		storeID := peer.GetStoreId()
		if store, ok := cache.leaders[storeID]; ok {
			store.remove(region.GetID())
		}
		if store, ok := cache.followers[storeID]; ok {
			store.remove(region.GetID())
		}
		if store, ok := cache.learners[storeID]; ok {
			store.remove(region.GetID())
		}
		if store, ok := cache.pendingPeers[storeID]; ok {
			store.remove(region.GetID())
		}
	}
}

// disable prevents the region from being selected again.
func (cache *regionCache) disable(regionID uint64) {
	if node := cache.regions.getNode(regionID); node != nil {
		node.rNode.available = false
		node.rNode.count++
		if node.rNode.count >= len(node.rNode.region.GetPeers()) {
			cache.remove(regionID)
		}
	}
}

// enable makes that the region can be selected again.
func (cache *regionCache) enable(regionID uint64) {
	if node := cache.regions.getNode(regionID); node != nil {
		node.rNode.available = true
	}
}

// randomRegion returns a random region from regionCache.
func (cache *regionCache) randomRegion(storeID uint64, ranges []KeyRange, optPending RegionOption, optOther RegionOption, optAll RegionOption, timeThreshold uint64) *RegionInfo {
	now := uint64(time.Now().Unix())
	if now-cache.lastTimestamp >= 10 && now-cache.lastTimestamp >= timeThreshold/100 {
		cache.lastTimestamp = now
		cache.Refresh(timeThreshold)
	}
	if len(ranges) == 0 {
		ranges = []KeyRange{NewKeyRange("", "")}
	}

	if store, ok := cache.pendingPeers[storeID]; ok {
		for _, node := range store.nodes {
			region := node.rNode.region
			if node.rNode.available && optPending(region) && optAll(region) && involved(region, ranges) {
				return region
			}
		}
	}
	if store, ok := cache.followers[storeID]; ok {
		for _, node := range store.nodes {
			region := node.rNode.region
			if node.rNode.available && optOther(region) && optAll(region) && involved(region, ranges) {
				return region
			}
		}
	}
	if store, ok := cache.leaders[storeID]; ok {
		for _, node := range store.nodes {
			region := node.rNode.region
			if node.rNode.available && optOther(region) && optAll(region) && involved(region, ranges) {
				return region
			}
		}
	}
	if store, ok := cache.learners[storeID]; ok {
		for _, node := range store.nodes {
			region := node.rNode.region
			if node.rNode.available && optOther(region) && optAll(region) && involved(region, ranges) {
				return region
			}
		}
	}
	return nil
}

func involved(region *RegionInfo, ranges []KeyRange) bool {
	for _, keyRange := range ranges {
		startKey := keyRange.StartKey
		endKey := keyRange.EndKey
		if len(startKey) > 0 && len(endKey) > 0 && bytes.Compare(startKey, endKey) > 0 {
			continue
		}
		if (len(startKey) == 0 || (len(region.GetStartKey()) > 0 && bytes.Compare(region.GetStartKey(), startKey) >= 0)) && (len(endKey) == 0 || (len(region.GetEndKey()) > 0 && bytes.Compare(region.GetEndKey(), endKey) <= 0)) {
			return true
		}
	}
	return true
}

// Refresh removes old regions from regionCache.
func (cache *regionCache) Refresh(timeThreshold uint64) {
	now := uint64(time.Now().Unix())
	temp := cache.regions.start
	for temp != nil {
		if temp.rNode.timestamp < now-timeThreshold {
			next := temp.next
			cache.remove(temp.rNode.region.GetID())
			temp = next
		} else {
			break
		}
	}
}
