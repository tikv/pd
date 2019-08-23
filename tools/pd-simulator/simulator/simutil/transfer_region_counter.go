package simutil

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

type TransferRegionCount struct {
	IsValid         bool
	regionMap       map[uint64]uint64
	graphMat        [][]uint64
	StoreNum        int
	RegionNum       int
	mutex           sync.Mutex
	visited         []bool
	loopResultPath  [][]int
	loopResultCount []uint64
}

var TransferRegionCounter TransferRegionCount

func (c *TransferRegionCount) Init(n, regionNum int) {
	c.StoreNum = n
	c.RegionNum = regionNum

	for i := 0; i < c.StoreNum+1; i++ {
		tmp := make([]uint64, c.StoreNum+1)
		c.graphMat = append(c.graphMat, tmp)
	}
	c.regionMap = make(map[uint64]uint64)
	c.visited = make([]bool, c.StoreNum+1)
	c.IsValid = true
}

func (c *TransferRegionCount) AddTarget(regionId, targetStoreId uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.regionMap[regionId] = targetStoreId
}

func (c *TransferRegionCount) AddSource(regionId, sourceStoreId uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if targetStoreId, ok := c.regionMap[regionId]; ok {
		c.graphMat[sourceStoreId][targetStoreId]++
		delete(c.regionMap, regionId)
	} else {
		Logger.Fatal("Error in map")
	}
}

func (c *TransferRegionCount) Print() {
	for _, value := range c.graphMat {
		fmt.Println(value)
	}
}

func (c *TransferRegionCount) DFS(cur int, curFlow uint64, path []int) {
	path = append(path, cur)
	c.visited[cur] = true

	for target := path[0]; target < c.StoreNum+1; target++ {
		flow := c.graphMat[cur][target]
		if flow == 0 {
			continue
		}
		if path[0] == target {
			curMinFlow := flow
			for i := 0; i < len(path)-1; i++ {
				pathFlow := c.graphMat[path[i]][path[i+1]]
				if curMinFlow > pathFlow {
					curMinFlow = pathFlow
				}
			}
			if curMinFlow != 0 {
				c.loopResultPath = append(c.loopResultPath, path)
				c.loopResultCount = append(c.loopResultCount, curMinFlow*uint64(len(path)))
				for i := 0; i < len(path)-1; i++ {
					c.graphMat[path[i]][path[i+1]] -= curMinFlow
				}
				c.graphMat[cur][target] -= curMinFlow
			}
		} else if !c.visited[target] {
			c.DFS(target, flow, path)
		}
	}

	path = path[0 : len(path)-1]
	c.visited[cur] = false
}

func (c *TransferRegionCount) Result() {
	for i := 0; i < c.StoreNum+1; i++ {
		c.DFS(i, 1<<16, make([]int, 0))
	}

	var redundant uint64
	for _, value := range c.loopResultCount {
		redundant += value
	}

	var necessary uint64
	for _, row := range c.graphMat {
		for _, flow := range row {
			necessary += flow
		}
	}
	for index, value := range c.loopResultPath {
		fmt.Println(index, value, c.loopResultCount[index])
	}
	c.Print()
	fmt.Println("redundant: ", redundant)
	fmt.Println("necessary: ", necessary)

	fd, _ := os.OpenFile("result.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	fdContent := strings.Join([]string{
		ToString(uint64(c.StoreNum)),
		ToString(uint64(c.RegionNum)),
		ToString(redundant),
		ToString(necessary),
	}, ",") + "\n"
	buf := []byte(fdContent)
	_, _ = fd.Write(buf)
	_ = fd.Close()
}

func ToString(num uint64) string {
	return strconv.FormatInt(int64(num), 10)
}
