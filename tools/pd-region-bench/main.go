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
	"github.com/tikv/pd/tools/utils"
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
	interval           = flag.Duration("interval", time.Second, "interval to output the statistics")
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
	// Validate the input parameters.
	if *clientNumber < 1 {
		log.Fatal("client-number must be greater than 0")
	}
	if *concurrency < 1 {
		log.Fatal("concurrency must be greater than 0")
	}
	if *round < 1 {
		log.Fatal("round must be greater than 0")
	}
	suite := newBenchmarkSuite(ctx)
	suite.prepare()
	// Start the multiple rounds of benchmark.
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
	regions   *utils.Regions
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
	pdCli := s.getPDClient()
	log.Info("bootstrap cluster")
	s.bootstrapCluster(s.ctx, pdCli)
	log.Info("prepare stores")
	s.prepareStores(s.ctx, pdCli)
	log.Info("prepare regions")
	s.prepareRegions(s.ctx, pdCli)
}

func (s *benchmarkSuite) getPDClient() pdpb.PDClient {
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
	return pdpb.NewPDClient(conn)
}

func (s *benchmarkSuite) bootstrapCluster(ctx context.Context, cli pdpb.PDClient) {
	utils.BootstrapCluster(ctx, cli, s.header(), version())
}

func (s *benchmarkSuite) prepareStores(ctx context.Context, cli pdpb.PDClient) {
	var stores []*metapb.Store
	for i := 1; i <= 3; i++ {
		storeID := uint64(i)
		stores = append(stores, &metapb.Store{
			Id:      storeID,
			Address: fmt.Sprintf("localhost:%d", storeID),
			Version: version(),
		})
	}
	utils.PutStores(ctx, cli, s.header(), stores)
}

func (s *benchmarkSuite) prepareRegions(ctx context.Context, cli pdpb.PDClient) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Generate the regions info first.
	s.regions = utils.NewRegions(*regionCount, 3, s.header())
	// Heartbeat the region.
	heartbeatStream := s.createHeartbeatStream(cctx, cli)
	for _, region := range s.regions.Regions {
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
			_, _ = stream.Recv()
		}
	}()

	return stream
}

func (s *benchmarkSuite) bench() {
	benchCtx, benchCancel := context.WithCancel(s.ctx)
	durCh := make(chan time.Duration, 2*(*concurrency)*(*clientNumber))
	s.wg.Add((*concurrency) * (*clientNumber))
	for i := range *clientNumber {
		for range *concurrency {
			go s.reqWorker(benchCtx, i, durCh)
		}
	}

	s.wg.Add(1)
	go utils.ShowStats(benchCtx, &s.wg, durCh, *interval, true, nil)

	timer := time.NewTimer(*duration)
	defer timer.Stop()
	select {
	case <-benchCtx.Done():
	case <-timer.C:
	}

	benchCancel()
}

func (s *benchmarkSuite) reqWorker(ctx context.Context, clientIdx int, durCh chan<- time.Duration) {
	defer s.wg.Done()
	pdCli := s.pdClients[clientIdx]
	// Create a local random generator to reduce contention.
	var (
		r     = rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientIdx)))
		start time.Time
	)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		durationSeed := r.Intn(100)
		regionSeed := r.Intn(*regionCount)
		regionReq := s.regions.Regions[regionSeed]

		// Wait for a random delay.
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(durationSeed) * time.Millisecond):
		}

		start = time.Now()
		// Invoke one of three PD client calls.
		switch durationSeed % 3 {
		case 0:
			_, _ = pdCli.GetRegion(ctx, regionReq.Region.GetStartKey())
		case 1:
			_, _ = pdCli.GetPrevRegion(ctx, regionReq.Region.GetStartKey())
		case 2:
			_, _ = pdCli.GetRegionByID(ctx, regionReq.Region.GetId())
		}
		dur := time.Since(start)
		select {
		case <-ctx.Done():
			return
		case durCh <- dur:
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
