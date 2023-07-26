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
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"go.etcd.io/etcd/pkg/report"
	"google.golang.org/grpc"
)

var (
	pdAddr            = flag.String("pd", "127.0.0.1:2379", "pd address")
	storeCount        = flag.Int("store", 20, "store count")
	regionCount       = flag.Uint64("region", 1000000, "region count")
	keyLen            = flag.Int("keylen", 56, "key length")
	replica           = flag.Int("replica", 3, "replica count")
	regionUpdateRatio = flag.Float64("region-update-ratio", 0.05, "ratio of the region need to update")
	sample            = flag.Bool("sample", false, "sample per second")
	heartbeatRounds   = flag.Int("heartbeat-rounds", 5, "total rounds of heartbeat")
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
	copy(k, []byte(fmt.Sprintf("%010d", id)))
	return k
}

func newEndKey(id uint64) []byte {
	k := newStartKey(id)
	k[len(k)-1]++
	return k
}

// Store simulates a TiKV to heartbeat.
type Store struct {
	id uint64
}

// Run runs the store.
func (s *Store) Run(startNotifier chan report.Report, endNotifier chan struct{}) {
	cli := newClient()
	stream, err := cli.RegionHeartbeat(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	var peers []*metapb.Peer
	for i := 0; i < *replica; i++ {
		storeID := s.id + uint64(i)
		if storeID > uint64(*storeCount) {
			storeID -= uint64(*storeCount)
		}
		peers = append(peers, &metapb.Peer{Id: uint64(i + 1), StoreId: storeID})
	}

	count := 1
	for r := range startNotifier {
		startTime := time.Now()
		for regionID := s.id; regionID <= *regionCount+uint64(*storeCount); regionID += uint64(*storeCount) {
			updateRegionCount := uint64(float64(*regionCount) * (*regionUpdateRatio) / float64(*storeCount))
			storeUpdateRegionMaxID := s.id + updateRegionCount*uint64(*storeCount)
			meta := &metapb.Region{
				Id:          regionID,
				Peers:       peers,
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 1},
				StartKey:    newStartKey(regionID),
				EndKey:      newEndKey(regionID),
			}
			if regionID < storeUpdateRegionMaxID {
				meta.RegionEpoch.Version = uint64(count)
			}
			reqStart := time.Now()
			err = stream.Send(&pdpb.RegionHeartbeatRequest{
				Header: header(),
				Region: meta,
				Leader: peers[0],
			})

			r.Results() <- report.Result{Start: reqStart, End: time.Now(), Err: err}
			if err != nil {
				log.Fatal(err)
			}
		}
		log.Printf("store %v finish heartbeat, cost time: %v", s.id, time.Since(startTime))
		count++
		endNotifier <- struct{}{}
	}
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
	for i := 1; i <= *storeCount; i++ {
		s := Store{id: uint64(i)}
		startNotifier := make(chan report.Report)
		endNotifier := make(chan struct{})
		groupStartNotify[i] = startNotifier
		groupEndNotify[i] = endNotifier
		go s.Run(startNotifier, endNotifier)
	}

	for i := 0; i < *heartbeatRounds; i++ {
		log.Printf("\n--------- Bench heartbeat (Round %d) ----------\n", i+1)
		report := newReport()
		rs := report.Run()
		// All stores start heartbeat.
		for storeID := 1; storeID <= *storeCount; storeID++ {
			startNotifier := groupStartNotify[storeID]
			startNotifier <- report
		}
		// All stores finished heartbeat once.
		for storeID := 1; storeID <= *storeCount; storeID++ {
			<-groupEndNotify[storeID]
		}

		close(report.Results())
		log.Println(<-rs)
	}
}
