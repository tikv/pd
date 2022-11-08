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
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	flag "github.com/spf13/pflag"
	"go.etcd.io/etcd/pkg/report"
	"google.golang.org/grpc"
)

var (
	pdAddr            = flag.String("pd", "127.0.0.1:2379", "pd address")
	storeCount        = flag.Int("store", 100, "store count")
	regionCount       = flag.Int("region", 1800000, "region count")
	keyLen            = flag.Int("key-len", 56, "key length")
	replica           = flag.Int("replica", 3, "replica count")
	leaderUpdateRatio = flag.Float64("leader", 0.06, "ratio of the region leader need to update, they need save-tree")
	epochUpdateRatio  = flag.Float64("epoch", 0.04, "ratio of the region epoch need to update, they need save-kv")
	spaceUpdateRatio  = flag.Float64("space", 0.15, "ratio of the region space need to update")
	flowUpdateRatio   = flag.Float64("flow", 0.35, "ratio of the region flow need to update")
	sample            = flag.Bool("sample", false, "sample per second")
	// for tiup
	logFile = flag.String("log-file", "", "")
)

const (
	bytesUnit    = 1 << 23 // 8MB
	keysUint     = 1 << 13 // 8K
	intervalUint = 60      // 60s
)

var clusterID uint64

func trimHTTPPrefix(str string) string {
	str = strings.TrimPrefix(str, "http://")
	str = strings.TrimPrefix(str, "https://")
	return str
}

func newClient() pdpb.PDClient {
	addr := trimHTTPPrefix(*pdAddr)
	cc, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	return pdpb.NewPDClient(cc)
}

func initClusterID(ctx context.Context, cli pdpb.PDClient) {
	res, err := cli.GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		log.Fatal(err)
	}
	if res.GetHeader().GetError() != nil {
		log.Fatal(res.GetHeader().GetError())
	}
	clusterID = res.GetHeader().GetClusterId()
	log.Println("ClusterID:", clusterID)
}

func header() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
}

func bootstrap(ctx context.Context, cli pdpb.PDClient) {
	isBootstrapped, err := cli.IsBootstrapped(ctx, &pdpb.IsBootstrappedRequest{Header: header()})
	if err != nil {
		log.Fatal(err)
	}
	if isBootstrapped.GetBootstrapped() {
		log.Println("already bootstrapped")
		return
	}

	store := &metapb.Store{
		Id:      1,
		Address: fmt.Sprintf("localhost:%d", 2),
		Version: "6.4.0-alpha",
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
	resp, err := cli.Bootstrap(ctx, req)
	if err != nil {
		log.Fatal(err)
	}
	if resp.GetHeader().GetError() != nil {
		log.Fatalf("bootstrap failed: %s", resp.GetHeader().GetError().String())
	}
	log.Println("bootstrapped")
}

func putStores(ctx context.Context, cli pdpb.PDClient) {
	for i := uint64(1); i <= uint64(*storeCount); i++ {
		store := &metapb.Store{
			Id:      i,
			Address: fmt.Sprintf("localhost:%d", i),
			Version: "6.4.0-alpha",
		}
		resp, err := cli.PutStore(ctx, &pdpb.PutStoreRequest{Header: header(), Store: store})
		if err != nil {
			log.Fatal(err)
		}
		if resp.GetHeader().GetError() != nil {
			log.Fatalf("put store failed: %s", resp.GetHeader().GetError().String())
		}
		go func(ctx context.Context, storeID uint64) {
			var heartbeatTicker = time.NewTicker(10 * time.Second)
			defer heartbeatTicker.Stop()
			for {
				select {
				case <-heartbeatTicker.C:
					cli.StoreHeartbeat(ctx, &pdpb.StoreHeartbeatRequest{Header: header(), Stats: &pdpb.StoreStats{
						StoreId:   storeID,
						Capacity:  2 * units.TiB,
						Available: 1.5 * units.TiB,
					}})
				case <-ctx.Done():
					return
				}
			}
		}(ctx, i)
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

func createHeartbeatStream(ctx context.Context) pdpb.PD_RegionHeartbeatClient {
	cli := newClient()
	stream, err := cli.RegionHeartbeat(ctx)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		// do nothing
		for {
			stream.Recv()
		}
	}()
	return stream
}

func (rs *Regions) handleRegionHeartbeat(wg *sync.WaitGroup, stream pdpb.PD_RegionHeartbeatClient, storeID uint64, rep report.Report) {
	defer wg.Done()
	var regions []*pdpb.RegionHeartbeatRequest
	for _, region := range rs.regions {
		if region.Leader.StoreId != storeID {
			continue
		}
		regions = append(regions, region)
	}

	start := time.Now()
	var err error
	for _, region := range regions {
		err = stream.Send(region)
		rep.Results() <- report.Result{Start: start, End: time.Now(), Err: err}
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("store %d finish one round region heartbeat, cost time: %v\n", storeID, time.Since(start))
}

func main() {
	log.SetFlags(0)
	flag.CommandLine.ParseErrorsWhitelist.UnknownFlags = true
	flag.Parse()

	// let PD have enough time to start
	time.Sleep(5 * time.Second)
	f, err := os.OpenFile(*logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("file open error : %v", err)
	}
	defer f.Close()
	log.SetOutput(f)
	ctx, cancel := context.WithCancel(context.Background())
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()
	cli := newClient()
	initClusterID(ctx, cli)
	bootstrap(ctx, cli)
	putStores(ctx, cli)
	log.Println("finish put stores")
	regions := new(Regions)
	regions.init()
	log.Println("finish init regions")

	streams := make(map[uint64]pdpb.PD_RegionHeartbeatClient, *storeCount)
	for i := 1; i <= *storeCount; i++ {
		streams[uint64(i)] = createHeartbeatStream(ctx)
	}
	var heartbeatTicker = time.NewTicker(60 * time.Second)
	defer heartbeatTicker.Stop()
	for {
		select {
		case <-heartbeatTicker.C:
			rep := newReport()
			r := rep.Run()

			startTime := time.Now()
			wg := &sync.WaitGroup{}
			for i := 1; i <= *storeCount; i++ {
				id := uint64(i)
				wg.Add(1)
				go regions.handleRegionHeartbeat(wg, streams[id], id, rep)
			}
			wg.Wait()

			since := time.Since(startTime).Seconds()
			close(rep.Results())
			log.Println(<-r)
			log.Println(regions.result(since))

			regions.update()
		case <-ctx.Done():
			log.Println("Got signal to exit")
			switch sig {
			case syscall.SIGTERM:
				exit(0)
			default:
				exit(1)
			}
		}
	}
}

func exit(code int) {
	os.Exit(code)
}

func newReport() report.Report {
	p := "%4.4f"
	if *sample {
		return report.NewReportSample(p)
	}
	return report.NewReport(p)
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
