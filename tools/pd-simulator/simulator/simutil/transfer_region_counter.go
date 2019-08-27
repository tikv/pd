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

package simutil

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// TransferRegionCount is to count transfer schedule for judging whether redundant
type TransferRegionCount struct {
	StoreNum        int
	RegionNum       int
	IsValid         bool
	Redundant       uint64
	Necessary       uint64
	regionMap       map[uint64]uint64
	visited         []bool
	GraphMat        [][]uint64
	mutex           sync.Mutex
	loopResultPath  [][]int
	loopResultCount []uint64
}

// TransferRegionCounter is an global instance for TransferRegionCount
var TransferRegionCounter TransferRegionCount

// Init for TransferRegionCount
func (c *TransferRegionCount) Init(n, regionNum int) {
	c.StoreNum = n
	c.RegionNum = regionNum
	c.IsValid = true
	c.Redundant = 0
	c.Necessary = 0
	c.regionMap = make(map[uint64]uint64)
	c.visited = make([]bool, c.StoreNum+1)
	for i := 0; i < c.StoreNum+1; i++ {
		tmp := make([]uint64, c.StoreNum+1)
		c.GraphMat = append(c.GraphMat, tmp)
	}
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
		c.GraphMat[sourceStoreID][targetStoreID]++
		delete(c.regionMap, regionID)
	} else {
		Logger.Fatal("Error in map")
	}
}

//DFS is used to find all the looped flow in such a directed graph.
//For each point U in the graph, a DFS is performed, and push the passing point v
//to the stack. If there is an edge of `v->u`, then the corresponding looped flow
//is marked and removed. When all the output edges of the point v are traversed,
//pop the point v out of the stack.
func (c *TransferRegionCount) DFS(cur int, curFlow uint64, path []int) {
	//push stack
	path = append(path, cur)
	c.visited[cur] = true

	for target := path[0]; target < c.StoreNum+1; target++ {
		flow := c.GraphMat[cur][target]
		if flow == 0 {
			continue
		}
		if path[0] == target { //is a loop
			//get curMinFlow
			curMinFlow := flow
			for i := 0; i < len(path)-1; i++ {
				pathFlow := c.GraphMat[path[i]][path[i+1]]
				if curMinFlow > pathFlow {
					curMinFlow = pathFlow
				}
			}
			//set curMinFlow
			if curMinFlow != 0 {
				c.loopResultPath = append(c.loopResultPath, path)
				c.loopResultCount = append(c.loopResultCount, curMinFlow*uint64(len(path)))
				for i := 0; i < len(path)-1; i++ {
					c.GraphMat[path[i]][path[i+1]] -= curMinFlow
				}
				c.GraphMat[cur][target] -= curMinFlow
			}
		} else if !c.visited[target] {
			c.DFS(target, flow, path)
		}
	}
	//pop stack
	c.visited[cur] = false
}

//Result will count redundant schedule and necessary schedule
func (c *TransferRegionCount) Result() {
	for i := 0; i < c.StoreNum; i++ {
		c.DFS(i+1, 1<<16, make([]int, 0))
	}

	for _, value := range c.loopResultCount {
		c.Redundant += value
	}

	for _, row := range c.GraphMat {
		for _, flow := range row {
			c.Necessary += flow
		}
	}
}

// PrintGraph will print current graph mat.
func (c *TransferRegionCount) PrintGraph() {
	for _, value := range c.GraphMat {
		fmt.Println(value)
	}
}

// PrintResult will print result to log and csv file.
func (c *TransferRegionCount) PrintResult() {
	//Output log
	fmt.Println("Redundant Loop: ")
	for index, value := range c.loopResultPath {
		fmt.Println(index, value, c.loopResultCount[index])
	}
	fmt.Println("Necessary: ")
	c.PrintGraph()
	fmt.Println("Redundant: ", c.Redundant)
	fmt.Println("Necessary: ", c.Necessary)

	//Output csv file
	fd, _ := os.OpenFile("result.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	fdContent := strings.Join([]string{
		toString(uint64(c.StoreNum)),
		toString(uint64(c.RegionNum)),
		toString(c.Redundant),
		toString(c.Necessary),
	}, ",") + "\n"
	buf := []byte(fdContent)
	_, _ = fd.Write(buf)
	_ = fd.Close()
}

func toString(num uint64) string {
	return strconv.FormatInt(int64(num), 10)
}
