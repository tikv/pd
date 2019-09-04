// Copyright 2019 PingCAP, Inc.
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

package analysis

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// TransferRegionCount is to count transfer schedule for judging whether redundant
type TransferRegionCount struct {
	storeNum          int
	scheduledStoreNum int
	regionNum         int
	IsValid           bool
	IsReady           bool
	Redundant         uint64
	Necessary         uint64
	regionMap         map[uint64]uint64
	visited           []bool
	graphMap          map[uint64]map[uint64]uint64
	graphMat          [][]uint64
	indexArray        []uint64
	unIndexMap        map[uint64]int
	mutex             sync.Mutex
	loopResultPath    [][]int
	loopResultCount   []uint64
}

var once sync.Once
var instance *TransferRegionCount

// GetTransferRegionCounter is to return singleTon for TransferRegionCount
func GetTransferRegionCounter() *TransferRegionCount {
	once.Do(func() {
		instance = &TransferRegionCount{}
	})
	return instance
}

// Init for TransferRegionCount
func (c *TransferRegionCount) Init(storeNum, regionNum int) {
	c.storeNum = storeNum
	c.scheduledStoreNum = 0
	c.regionNum = regionNum
	c.IsValid = true
	c.IsReady = false
	c.Redundant = 0
	c.Necessary = 0
	c.regionMap = make(map[uint64]uint64)
	c.unIndexMap = make(map[uint64]int)
	c.graphMap = make(map[uint64]map[uint64]uint64)
	c.loopResultPath = c.loopResultPath[:0]
	c.loopResultCount = c.loopResultCount[:0]
}

// AddTarget is be used to add target of edge in graph mat.
// Firstly add a new peer and then delete the old peer of the scheduling,
// So in the statistics, also firstly add the target and then add the source.
func (c *TransferRegionCount) AddTarget(regionID, targetStoreID uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.regionMap[regionID] = targetStoreID
}

// AddSource is be used to add source of edge in graph mat.
func (c *TransferRegionCount) AddSource(regionID, sourceStoreID uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if targetStoreID, ok := c.regionMap[regionID]; ok {
		if edge, ok := c.graphMap[sourceStoreID]; ok {
			edge[targetStoreID]++
		} else {
			edge = make(map[uint64]uint64)
			edge[targetStoreID]++
			c.graphMap[sourceStoreID] = edge
		}
		delete(c.regionMap, regionID)
	} else {
		fmt.Println("Error when add sourceStoreID %u with regionID %u in transfer region map", sourceStoreID, regionID)
		os.Exit(-1)
	}
}

// prepare is to change sparse map to dense mat.
func (c *TransferRegionCount) prepare() {
	c.IsReady = true
	set := make(map[uint64]struct{})
	for sourceID, edge := range c.graphMap {
		for targetID := range edge {
			set[sourceID] = struct{}{}
			set[targetID] = struct{}{}
		}
	}

	c.scheduledStoreNum = len(set)
	c.visited = make([]bool, c.scheduledStoreNum+1)
	c.indexArray = make([]uint64, 0, c.scheduledStoreNum)
	for storeID := range set {
		c.indexArray = append(c.indexArray, storeID)
	}
	sort.Slice(c.indexArray, func(i, j int) bool { return c.indexArray[i] < c.indexArray[j] })

	for index, storeID := range c.indexArray {
		c.unIndexMap[storeID] = index
	}

	c.graphMat = make([][]uint64, 0)
	for i := 0; i < c.scheduledStoreNum; i++ {
		tmp := make([]uint64, c.scheduledStoreNum)
		c.graphMat = append(c.graphMat, tmp)
	}

	for sourceID, edge := range c.graphMap {
		for targetID, flow := range edge {
			sourceIndex := c.unIndexMap[sourceID]
			targetIndex := c.unIndexMap[targetID]
			c.graphMat[sourceIndex][targetIndex] = flow
		}
	}
}

// dfs is used to find all the looped flow in such a directed graph.
// For each point U in the graph, a DFS is performed, and push the passing point v
// to the stack. If there is an edge of `v->u`, then the corresponding looped flow
// is marked and removed. When all the output edges of the point v are traversed,
// pop the point v out of the stack.
func (c *TransferRegionCount) dfs(cur int, curFlow uint64, path []int) {
	// push stack
	path = append(path, cur)
	c.visited[cur] = true

	for target := path[0]; target < c.scheduledStoreNum; target++ {
		flow := c.graphMat[cur][target]
		if flow == 0 {
			continue
		}
		if path[0] == target { //is a loop
			// get curMinFlow
			curMinFlow := flow
			for i := 0; i < len(path)-1; i++ {
				pathFlow := c.graphMat[path[i]][path[i+1]]
				if curMinFlow > pathFlow {
					curMinFlow = pathFlow
				}
			}
			// set curMinFlow
			if curMinFlow != 0 {
				c.loopResultPath = append(c.loopResultPath, path)
				c.loopResultCount = append(c.loopResultCount, curMinFlow*uint64(len(path)))
				for i := 0; i < len(path)-1; i++ {
					c.graphMat[path[i]][path[i+1]] -= curMinFlow
				}
				c.graphMat[cur][target] -= curMinFlow
			}
		} else if !c.visited[target] {
			c.dfs(target, flow, path)
		}
	}
	// pop stack
	c.visited[cur] = false
}

// Result will count redundant schedule and necessary schedule
func (c *TransferRegionCount) Result() {
	if !c.IsReady {
		c.prepare()
	}

	for i := 0; i < c.scheduledStoreNum; i++ {
		c.dfs(i, 1<<16, make([]int, 0))
	}

	for _, value := range c.loopResultCount {
		c.Redundant += value
	}

	for _, row := range c.graphMat {
		for _, flow := range row {
			c.Necessary += flow
		}
	}
}

// printGraph will print current graph mat.
func (c *TransferRegionCount) printGraph() {
	fmt.Print("\t")
	for _, storeID := range c.indexArray {
		fmt.Print(storeID, "\t")
	}
	fmt.Println()
	for index, row := range c.graphMat {
		fmt.Print(c.indexArray[index], "\t")
		for _, flow := range row {
			fmt.Print(flow, "\t")
		}
		fmt.Println()
	}
}

// PrintResult will print result to log and csv file.
func (c *TransferRegionCount) PrintResult() {
	c.prepare()
	// Output log
	fmt.Println("Total Schedules Graph: ")
	c.printGraph()
	// Solve data
	c.Result()
	// Output log
	fmt.Println("Redundant Loop: ")
	for index, value := range c.loopResultPath {
		fmt.Println(index, value, c.loopResultCount[index])
	}
	fmt.Println("Necessary Schedules Graph: ")
	c.printGraph()
	fmt.Println("Scheduled Store: ", c.scheduledStoreNum)
	fmt.Println("Redundant Schedules: ", c.Redundant)
	fmt.Println("Necessary Schedules: ", c.Necessary)

	// Output csv file
	fd, err := os.OpenFile("result.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer fd.Close()
	fdContent := strings.Join([]string{
		toString(uint64(c.storeNum)),
		toString(uint64(c.regionNum)),
		toString(c.Redundant),
		toString(c.Necessary),
	}, ",") + "\n"
	buf := []byte(fdContent)
	_, err = fd.Write(buf)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
}

func toString(num uint64) string {
	return strconv.FormatInt(int64(num), 10)
}
