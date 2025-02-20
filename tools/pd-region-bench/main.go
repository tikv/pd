// Copyright 2025 TiKV Project Authors.
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
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/pkg/codec"
)

const (
	grpcKeepaliveTime    = 10 * time.Second
	grpcKeepaliveTimeout = 3 * time.Second
)

var (
	pdAddrs            = flag.String("pd", "127.0.0.1:2379", "pd address")
	regionCount        = flag.Int("region-count", 10000, "the number of regions to prepare")
	clientNumber       = flag.Int("client", 1, "the number of pd clients involved in each benchmark")
	concurrency        = flag.Int("c", 1000, "concurrency")
	round              = flag.Int("round", 1, "the number of rounds that the test will run")
	duration           = flag.Duration("duration", 60*time.Second, "how many seconds the test will last")
	caPath             = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath           = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath            = flag.String("key", "", "path of file that contains X509 key in PEM format")
	enableRouterClient = flag.Bool("enable-router-client", false, "whether enable the router client")
)

func main() {
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-sc
		cancel()
	}()

	log.Info("prepare benchmark suite",
		zap.Int("client-number", *clientNumber),
		zap.Int("concurrency", *concurrency),
		zap.Duration("duration", *duration),
		zap.Int("round", *round))
	suite := newBenchmarkSuite(ctx)
	suite.prepare()
	for i := range *round {
		log.Info("start benchmark", zap.Int("round", i), zap.Duration("duration", *duration))
		suite.bench()
	}
	suite.cleanup()
}

type benchmarkSuite struct {
	ctx       context.Context
	cancel    context.CancelFunc
	clusterID uint64
	regions   []*pdpb.RegionHeartbeatRequest
	wg        sync.WaitGroup
	pdClients []pd.Client
}

func newBenchmarkSuite(ctx context.Context) *benchmarkSuite {
	suite := &benchmarkSuite{
		pdClients: make([]pd.Client, *clientNumber),
	}
	suite.ctx, suite.cancel = context.WithCancel(ctx)
	// Initialize all clients.
	log.Info("create pd clients", zap.Int("number", *clientNumber))
	for idx := range suite.pdClients {
		pdCli, err := createPDClient(suite.ctx)
		if err != nil {
			log.Fatal("create pd client failed", zap.Int("client-idx", idx), zap.Error(err))
		}
		suite.pdClients[idx] = pdCli
	}
	// Initialize the cluster ID.
	suite.clusterID = suite.pdClients[0].GetClusterID(suite.ctx)
	return suite
}

func createPDClient(ctx context.Context) (pd.Client, error) {
	opts := make([]opt.ClientOption, 0)
	if *enableRouterClient {
		opts = append(opts, opt.WithEnableRouterClient(true))
	}
	opts = append(opts, opt.WithGRPCDialOptions(
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    grpcKeepaliveTime,
			Timeout: grpcKeepaliveTimeout,
		}),
	))

	return pd.NewClientWithContext(ctx,
		caller.TestComponent,
		[]string{*pdAddrs},
		pd.SecurityOption{
			CAPath:   *caPath,
			CertPath: *certPath,
			KeyPath:  *keyPath,
		}, opts...)
}

func (s *benchmarkSuite) header() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: s.clusterID,
	}
}

func version() string {
	return "9.0.0-alpha"
}

func (s *benchmarkSuite) prepare() {
	if len(s.pdClients) == 0 {
		log.Fatal("no pd client initialized to prepare heartbeat stream")
	}
	cli := s.pdClients[0]
	if cli == nil {
		log.Fatal("got a nil pd client before creating heartbeat stream")
	}
	conn := cli.GetServiceDiscovery().GetServingEndpointClientConn()
	if conn == nil {
		log.Fatal("got a nil grpc connection before creating heartbeat stream")
	}
	pdCli := pdpb.NewPDClient(conn)
	log.Info("bootstrap cluster")
	s.bootstrapCluster(s.ctx, pdCli)
	log.Info("prepare stores")
	s.prepareStores(s.ctx, pdCli)
	log.Info("prepare regions")
	s.prepareRegions(s.ctx, pdCli)
}

func (s *benchmarkSuite) bootstrapCluster(ctx context.Context, cli pdpb.PDClient) {
	cctx, cancel := context.WithCancel(ctx)
	isBootstrapped, err := cli.IsBootstrapped(cctx, &pdpb.IsBootstrappedRequest{Header: s.header()})
	cancel()
	if err != nil {
		log.Fatal("check if cluster has already bootstrapped failed", zap.Error(err))
	}
	if isBootstrapped.GetBootstrapped() {
		log.Info("already bootstrapped")
		return
	}

	store := &metapb.Store{
		Id:      1,
		Address: fmt.Sprintf("localhost:%d", 2),
		Version: version(),
	}
	region := &metapb.Region{
		Id:          1,
		Peers:       []*metapb.Peer{{StoreId: 1, Id: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	req := &pdpb.BootstrapRequest{
		Header: s.header(),
		Store:  store,
		Region: region,
	}
	cctx, cancel = context.WithCancel(ctx)
	resp, err := cli.Bootstrap(cctx, req)
	cancel()
	if err != nil {
		log.Fatal("failed to bootstrap the cluster", zap.Error(err))
	}
	if resp.GetHeader().GetError() != nil {
		log.Fatal("failed to bootstrap the cluster", zap.String("err", resp.GetHeader().GetError().String()))
	}
}

func (s *benchmarkSuite) prepareStores(ctx context.Context, cli pdpb.PDClient) {
	for i := 1; i <= 3; i++ {
		storeID := uint64(i)
		store := &metapb.Store{
			Id:      storeID,
			Address: fmt.Sprintf("localhost:%d", storeID),
			Version: version(),
		}
		cctx, cancel := context.WithCancel(ctx)
		resp, err := cli.PutStore(cctx, &pdpb.PutStoreRequest{Header: s.header(), Store: store})
		cancel()
		if err != nil {
			log.Fatal("failed to put store", zap.Uint64("store-id", storeID), zap.Error(err))
		}
		if resp.GetHeader().GetError() != nil {
			log.Fatal("failed to put store", zap.Uint64("store-id", storeID), zap.String("err", resp.GetHeader().GetError().String()))
		}

		go func(ctx context.Context, storeID uint64) {
			heartbeatTicker := time.NewTicker(10 * time.Second)
			defer heartbeatTicker.Stop()
			for {
				select {
				case <-heartbeatTicker.C:
					cctx, cancel := context.WithCancel(ctx)
					defer cancel()
					cli.StoreHeartbeat(cctx, &pdpb.StoreHeartbeatRequest{
						Header: s.header(),
						Stats: &pdpb.StoreStats{
							StoreId: storeID,
						},
					})
				case <-ctx.Done():
					return
				}
			}
		}(ctx, storeID)
	}
}

func (s *benchmarkSuite) prepareRegions(ctx context.Context, cli pdpb.PDClient) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	heartbeatStream := s.createHeartbeatStream(cctx, cli)
	// Generate the regions info first.
	s.regions = make([]*pdpb.RegionHeartbeatRequest, 0, *regionCount)
	id := uint64(1)
	for i := range *regionCount {
		region := &pdpb.RegionHeartbeatRequest{
			Header: s.header(),
			Region: &metapb.Region{
				Id:          id,
				StartKey:    codec.GenerateTableKey(int64(i)),
				EndKey:      codec.GenerateTableKey(int64(i + 1)),
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
			},
		}
		id += 1
		// Specially handle the first and last region.
		if i == 0 {
			region.Region.StartKey = []byte("")
		}
		if i == *regionCount-1 {
			region.Region.EndKey = []byte("")
		}
		peers := make([]*metapb.Peer, 0, 3)
		for j := 1; j <= 3; j++ {
			peers = append(peers, &metapb.Peer{Id: id, StoreId: uint64((i+j)%3 + 1)})
			id += 1
		}

		region.Region.Peers = peers
		region.Leader = peers[0]
		s.regions = append(s.regions, region)
		// Heartbeat the region.
		err := heartbeatStream.Send(region)
		if err != nil {
			log.Fatal("send region heartbeat request error", zap.Error(err))
		}
	}
}

func (s *benchmarkSuite) createHeartbeatStream(ctx context.Context, pdCli pdpb.PDClient) pdpb.PD_RegionHeartbeatClient {
	stream, err := pdCli.RegionHeartbeat(ctx)
	if err != nil {
		log.Fatal("create heartbeat stream error", zap.Error(err))
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			stream.Recv()
		}
	}()

	return stream
}

func (s *benchmarkSuite) bench() {
	benchCtx, benchCancel := context.WithCancel(s.ctx)
	s.wg.Add((*concurrency) * (*clientNumber))
	for i := range *clientNumber {
		for range *concurrency {
			go s.reqWorker(benchCtx, i)
		}
	}

	timer := time.NewTimer(*duration)
	defer timer.Stop()
	select {
	case <-benchCtx.Done():
	case <-timer.C:
	}

	benchCancel()
}

func (s *benchmarkSuite) reqWorker(ctx context.Context, clientIdx int) {
	defer s.wg.Done()
	pdCli := s.pdClients[clientIdx]
	// Create a local random generator to reduce contention.
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientIdx)))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		durationSeed := r.Intn(100)
		regionSeed := r.Intn(*regionCount)
		regionReq := s.regions[regionSeed]

		// Wait for a random delay.
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(durationSeed) * time.Millisecond):
		}

		// Invoke one of three PD client calls.
		switch durationSeed % 3 {
		case 0:
			pdCli.GetRegion(ctx, regionReq.Region.GetStartKey())
		case 1:
			pdCli.GetPrevRegion(ctx, regionReq.Region.GetStartKey())
		case 2:
			pdCli.GetRegionByID(ctx, regionReq.Region.GetId())
		}
		// TODO: Optionally verify that the returned region is correct.
	}
}

func (s *benchmarkSuite) cleanup() {
	s.wg.Wait()

	for idx := range s.pdClients {
		s.pdClients[idx].Close()
	}
}
