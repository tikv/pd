// Copyright 2019 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"go.etcd.io/etcd/pkg/report"
	"google.golang.org/grpc"
)

var (
	pdAddr            = flag.String("pd", "127.0.0.1:2379", "pd address")
	storeCount        = flag.Int("store", 40, "store count")
	regionCount       = flag.Int("region", 1000000, "region count")
	keyLen            = flag.Int("key-len", 56, "key length")
	replica           = flag.Int("replica", 3, "replica count")
	leaderUpdateRatio = flag.Float64("leader", 0.06, "ratio of the region leader need to update, they need save-tree")
	epochUpdateRatio  = flag.Float64("epoch", 0.04, "ratio of the region epoch need to update, they need save-kv")
	spaceUpdateRatio  = flag.Float64("space", 0.15, "ratio of the region space need to update")
	flowUpdateRatio   = flag.Float64("flow", 0.35, "ratio of the region flow need to update")
	sample            = flag.Bool("sample", false, "sample per second")
	heartbeatRounds   = flag.Int("heartbeat-rounds", 4, "total rounds of heartbeat")
)

const (
	bytesUnit    = 1 << 23 // 8MB
	keysUint     = 1 << 13 // 8K
	intervalUint = 60      // 60s
)

var clusterID uint64

func newClient() pdpb.PDClient {
	cc, err := grpc.Dial(*pdAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	return pdpb.NewPDClient(cc)
}

func newReport() report.Report {
	p := "%4.4f"
	if *sample {
		return report.NewReportSample(p)
	}
	return report.NewReport(p)
}

func initClusterID(cli pdpb.PDClient) {
	res, err := cli.GetMembers(context.TODO(), &pdpb.GetMembersRequest{})
	if err != nil {
		log.Fatal(err)
	}
	clusterID = res.GetHeader().GetClusterId()
	log.Println("ClusterID:", clusterID)
}

func header() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
}

func bootstrap(cli pdpb.PDClient) {
	isBootstrapped, err := cli.IsBootstrapped(context.TODO(), &pdpb.IsBootstrappedRequest{Header: header()})
	if err != nil {
		log.Fatal(err)
	}
	if isBootstrapped.GetBootstrapped() {
		log.Println("already bootstrapped")
		return
	}

	store := &metapb.Store{
		Id:      1,
		Address: fmt.Sprintf("localhost:%d", 1),
	}
	region := &metapb.Region{
		Id:          1,
		Peers:       []*metapb.Peer{{StoreId: 1, Id: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	req := &pdpb.BootstrapRequest{
		Header: header(),
		Store:  store,
		Region: region,
	}
	_, err = cli.Bootstrap(context.TODO(), req)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("bootstrapped")
}

func putStores(cli pdpb.PDClient) {
	for i := uint64(1); i <= uint64(*storeCount); i++ {
		store := &metapb.Store{
			Id:      i,
			Address: fmt.Sprintf("localhost:%d", i),
		}
		_, err := cli.PutStore(context.TODO(), &pdpb.PutStoreRequest{Header: header(), Store: store})
		if err != nil {
			log.Fatal(err)
		}
	}
}

func newStartKey(id uint64) []byte {
	k := make([]byte, *keyLen)
	copy(k, fmt.Sprintf("%010d", id))
	return k
}

func newEndKey(id uint64) []byte {
	k := newStartKey(id)
	k[len(k)-1]++
	return k
}

// Regions simulates all regions to heartbeat.
type Regions struct {
	regions []*pdpb.RegionHeartbeatRequest

	updateRound int

	updateLeader []int
	updateEpoch  []int
	updateSpace  []int
	updateFlow   []int
}

func (rs *Regions) init() {
	rs.regions = make([]*pdpb.RegionHeartbeatRequest, 0, *regionCount)
	rs.updateRound = 0

	// Generate regions
	id := uint64(1)
	now := uint64(time.Now().Unix())

	for i := 0; i < *regionCount; i++ {
		region := &pdpb.RegionHeartbeatRequest{
			Header: header(),
			Region: &metapb.Region{
				Id:          id,
				StartKey:    newStartKey(id),
				EndKey:      newEndKey(id),
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 1},
			},
			ApproximateSize: bytesUnit,
			Interval: &pdpb.TimeInterval{
				StartTimestamp: now,
				EndTimestamp:   now + intervalUint,
			},
			ApproximateKeys: keysUint,
			Term:            1,
		}
		id += 1

		peers := make([]*metapb.Peer, 0, *replica)
		for j := 0; j < *replica; j++ {
			peers = append(peers, &metapb.Peer{Id: id, StoreId: uint64((i+j)%*storeCount + 1)})
			id += 1
		}

		region.Region.Peers = peers
		region.Leader = peers[0]
		rs.regions = append(rs.regions, region)
	}

	// Generate sample index
	slice := make([]int, *regionCount)
	for i := range slice {
		slice[i] = i
	}

	rand.Seed(0) // Ensure consistent behavior multiple times
	pick := func(ratio float64) []int {
		rand.Shuffle(*regionCount, func(i, j int) {
			slice[i], slice[j] = slice[j], slice[i]
		})
		return append(slice[:0:0], slice[0:int(float64(*regionCount)*ratio)]...)
	}

	rs.updateLeader = pick(*leaderUpdateRatio)
	rs.updateEpoch = pick(*epochUpdateRatio)
	rs.updateSpace = pick(*spaceUpdateRatio)
	rs.updateFlow = pick(*flowUpdateRatio)
}

func (rs *Regions) update() {
	rs.updateRound += 1

	// update leader
	for _, i := range rs.updateLeader {
		region := rs.regions[i]
		region.Leader = region.Region.Peers[rs.updateRound%*replica]
	}
	// update epoch
	for _, i := range rs.updateEpoch {
		region := rs.regions[i]
		region.Region.RegionEpoch.Version += 1
	}
	// update space
	for _, i := range rs.updateSpace {
		region := rs.regions[i]
		region.ApproximateSize += bytesUnit
		region.ApproximateKeys += keysUint
	}
	// update flow
	for _, i := range rs.updateFlow {
		region := rs.regions[i]
		region.BytesWritten += bytesUnit
		region.BytesRead += bytesUnit
		region.KeysWritten += keysUint
		region.KeysRead += keysUint
	}
	// update interval
	for _, region := range rs.regions {
		region.Interval.StartTimestamp = region.Interval.EndTimestamp
		region.Interval.EndTimestamp = region.Interval.StartTimestamp + intervalUint
	}
}

func (rs *Regions) send(storeID uint64, startNotifier chan report.Report, endNotifier chan struct{}) {
	cli := newClient()
	stream, err := cli.RegionHeartbeat(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	for r := range startNotifier {
		startTime := time.Now()
		count := 0
		for _, region := range rs.regions {
			if region.Leader.StoreId != storeID {
				continue
			}
			count += 1
			reqStart := time.Now()
			err = stream.Send(region)
			r.Results() <- report.Result{Start: reqStart, End: time.Now(), Err: err}
			if err != nil {
				log.Fatal(err)
			}
		}
		log.Printf("store %v finish heartbeat, count: %v, cost time: %v", storeID, count, time.Since(startTime))
		endNotifier <- struct{}{}
	}
}

func (rs *Regions) result(sec float64) string {
	if rs.updateRound == 0 {
		// There was no difference in the first round
		return ""
	}

	updated := make(map[int]struct{})
	for _, i := range rs.updateLeader {
		updated[i] = struct{}{}
	}
	for _, i := range rs.updateEpoch {
		updated[i] = struct{}{}
	}
	for _, i := range rs.updateSpace {
		updated[i] = struct{}{}
	}
	for _, i := range rs.updateFlow {
		updated[i] = struct{}{}
	}
	inactiveCount := *regionCount - len(updated)

	ret := "Update speed of each category:\n"
	ret += fmt.Sprintf("  Requests/sec:   %12.4f\n", float64(*regionCount)/sec)
	ret += fmt.Sprintf("  Save-Tree/sec:  %12.4f\n", float64(len(rs.updateLeader))/sec)
	ret += fmt.Sprintf("  Save-KV/sec:    %12.4f\n", float64(len(rs.updateEpoch))/sec)
	ret += fmt.Sprintf("  Save-Space/sec: %12.4f\n", float64(len(rs.updateSpace))/sec)
	ret += fmt.Sprintf("  Save-Flow/sec:  %12.4f\n", float64(len(rs.updateFlow))/sec)
	ret += fmt.Sprintf("  Skip/sec:       %12.4f\n", float64(inactiveCount)/sec)
	return ret
}

func main() {
	log.SetFlags(0)
	flag.Parse()

	cli := newClient()
	initClusterID(cli)
	bootstrap(cli)
	putStores(cli)

	log.Println("finish put stores")
	groupStartNotify := make([]chan report.Report, *storeCount+1)
	groupEndNotify := make([]chan struct{}, *storeCount+1)
	regions := new(Regions)
	regions.init()

	for i := 1; i <= *storeCount; i++ {
		startNotifier := make(chan report.Report)
		endNotifier := make(chan struct{})
		groupStartNotify[i] = startNotifier
		groupEndNotify[i] = endNotifier
		go regions.send(uint64(i), startNotifier, endNotifier)
	}

	for i := 0; i < *heartbeatRounds; i++ {
		log.Printf("\n--------- Bench heartbeat (Round %d) ----------\n", i+1)
		repo := newReport()
		rs := repo.Run()
		// All stores start heartbeat.
		startTime := time.Now()
		for storeID := 1; storeID <= *storeCount; storeID++ {
			startNotifier := groupStartNotify[storeID]
			startNotifier <- repo
		}
		// All stores finished heartbeat once.
		for storeID := 1; storeID <= *storeCount; storeID++ {
			<-groupEndNotify[storeID]
		}
		since := time.Since(startTime).Seconds()
		close(repo.Results())
		log.Println(<-rs)
		log.Println(regions.result(since))
		regions.update()
	}
}
