package simutil

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

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

var TransferRegionCounter TransferRegionCount

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

// Firstly add a new peer and then delete the old peer of the scheduling,
// So in the statistics, also firstly add the target and then add the source.
func (c *TransferRegionCount) AddTarget(regionId, targetStoreId uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.regionMap[regionId] = targetStoreId
}

func (c *TransferRegionCount) AddSource(regionId, sourceStoreId uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if targetStoreId, ok := c.regionMap[regionId]; ok {
		c.GraphMat[sourceStoreId][targetStoreId]++
		delete(c.regionMap, regionId)
	} else {
		Logger.Fatal("Error in map")
	}
}

//A simple DFS is used to find all the looped flow in such a directed graph.
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

//Output Count Result
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

func (c *TransferRegionCount) PrintGraph() {
	for _, value := range c.GraphMat {
		fmt.Println(value)
	}
}

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
		ToString(uint64(c.StoreNum)),
		ToString(uint64(c.RegionNum)),
		ToString(c.Redundant),
		ToString(c.Necessary),
	}, ",") + "\n"
	buf := []byte(fdContent)
	_, _ = fd.Write(buf)
	_ = fd.Close()
}

func ToString(num uint64) string {
	return strconv.FormatInt(int64(num), 10)
}
