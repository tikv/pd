package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/tikv/pd/server/api"
)

func main() {
	var regionsFile, storeFile string
	flag.StringVar(&regionsFile, "region", "", "")
	flag.StringVar(&storeFile, "store", "", "")
	flag.Parse()
	storesB, _ := os.ReadFile(storeFile)
	regionB, _ := os.ReadFile(regionsFile)
	stores := &api.StoresInfo{}
	err := json.Unmarshal(storesB, stores)
	if err != nil {
		fmt.Println("1", err)
		return
	}
	regions := &api.RegionsInfo{}
	err = json.Unmarshal(regionB, &regions)
	if err != nil {
		fmt.Println("2", err)
		return
	}
	pc := 0
	dc := 0
	tp := 0
	pendingCount := make(map[uint64]int)
	downCount := make(map[uint64]int)
	for _, region := range regions.Regions {
		if len(region.PendingPeers) > 0 {
			pc++
		}
		v := 0
		for _, p := range region.PendingPeers {
			pendingCount[p.StoreId]++
			if p.IsLearner {
				v++
			}
		}
		if v > 1 {
			tp++
			fmt.Println(region.ID, region.PendingPeers)
		}
		if len(region.DownPeers) > 0 {
			dc++
		}
		for _, d := range region.DownPeers {
			downCount[d.Peer.StoreId]++
		}
	}
	maxpc := 0
	maxpcId := uint64(0)
	minpc := 10000000
	minpcId := uint64(0)
	for id, c := range pendingCount {
		if c > maxpc {
			maxpc = c
			maxpcId = id
		}
		if c < minpc {
			minpc = c
			minpcId = id
		}
	}
	maxdc := 0
	mindc := 10000000
	for _, c := range downCount {
		if c > maxdc {
			maxdc = c
		}
		if c < mindc {
			mindc = c
		}
	}
	fmt.Println(pc, maxpc, maxpcId, minpc, minpcId, len(pendingCount), pendingCount)
	fmt.Println(dc, maxdc, mindc, len(downCount), downCount)
	fmt.Println(tp)
	fmt.Printf("%x\n", uint64(2234275743217008298))

}
