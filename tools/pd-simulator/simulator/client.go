// Copyright 2017 TiKV Project Authors.
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

package simulator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	pdHttp "github.com/tikv/pd/client/http"
	sd "github.com/tikv/pd/client/servicediscovery"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/typeutil"
	sc "github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
)

// Client is a PD (Placement Driver) client.
// It should not be used after calling Close().
type Client interface {
	allocID(context.Context) (uint64, error)
	PutStore(context.Context, *metapb.Store) error
	StoreHeartbeat(context.Context, *pdpb.StoreStats) error
	RegionHeartbeat(context.Context, *core.RegionInfo) error

	heartbeatStreamLoop()
	changeConn(*grpc.ClientConn) error
	Close()
}

const (
	pdTimeout             = 3 * time.Second
	maxInitClusterRetries = 100
	// retry to get leader URL
	leaderChangedWaitTime = 100 * time.Millisecond
	retryTimes            = 10
)

var (
	// errFailInitClusterID is returned when failed to load clusterID from all supplied PD addresses.
	errFailInitClusterID = errors.New("[pd] failed to get cluster id")
	// PDHTTPClient is a client for PD HTTP API.
	PDHTTPClient pdHttp.Client
	// SD is a service discovery for PD.
	SD        sd.ServiceDiscovery
	clusterID atomic.Uint64
)

// requestHeader returns a header for fixed ClusterID.
func requestHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: clusterID.Load(),
	}
}

type client struct {
	tag        string
	clientConn *grpc.ClientConn

	reportRegionHeartbeatCh  chan *core.RegionInfo
	receiveRegionHeartbeatCh chan *pdpb.RegionHeartbeatResponse

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewClient creates a PD client.
func NewClient(tag string) (Client, <-chan *pdpb.RegionHeartbeatResponse, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		reportRegionHeartbeatCh:  make(chan *core.RegionInfo, 1),
		receiveRegionHeartbeatCh: make(chan *pdpb.RegionHeartbeatResponse, 1),
		ctx:                      ctx,
		cancel:                   cancel,
		tag:                      tag,
	}
	return c, c.receiveRegionHeartbeatCh, nil
}

func (c *client) pdClient() pdpb.PDClient {
	return pdpb.NewPDClient(c.clientConn)
}

func createConn(url string) (*grpc.ClientConn, error) {
	cc, err := grpc.Dial(strings.TrimPrefix(url, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return cc, nil
}

func (c *client) changeConn(cc *grpc.ClientConn) error {
	c.clientConn = cc
	simutil.Logger.Info("change pd client with endpoints", zap.String("tag", c.tag), zap.String("pd-address", cc.Target()))
	return nil
}

func (c *client) createHeartbeatStream() (pdpb.PD_RegionHeartbeatClient, context.Context, context.CancelFunc) {
	var (
		stream pdpb.PD_RegionHeartbeatClient
		err    error
		cancel context.CancelFunc
		ctx    context.Context
	)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		ctx, cancel = context.WithCancel(c.ctx)
		stream, err = c.pdClient().RegionHeartbeat(ctx)
		if err == nil {
			break
		}
		simutil.Logger.Error("create region heartbeat stream error", zap.String("tag", c.tag), zap.Error(err))
		cancel()
		select {
		case <-c.ctx.Done():
			simutil.Logger.Info("cancel create stream loop")
			return nil, ctx, cancel
		case <-ticker.C:
		}
	}
	return stream, ctx, cancel
}

func (c *client) heartbeatStreamLoop() {
	c.wg.Add(1)
	defer c.wg.Done()
	for {
		stream, ctx, cancel := c.createHeartbeatStream()
		if stream == nil {
			return
		}
		errCh := make(chan error, 1)
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go c.reportRegionHeartbeat(ctx, stream, errCh, wg)
		go c.receiveRegionHeartbeat(ctx, stream, errCh, wg)
		select {
		case err := <-errCh:
			simutil.Logger.Error("heartbeat stream get error", zap.String("tag", c.tag), zap.Error(err))
			cancel()
		case <-c.ctx.Done():
			simutil.Logger.Info("cancel heartbeat stream loop")
			return
		}
		wg.Wait()

		// update connection to recreate heartbeat stream
		for range retryTimes {
			SD.ScheduleCheckMemberChanged()
			time.Sleep(leaderChangedWaitTime)
			if client := SD.GetServiceClient(); client != nil {
				_, conn, err := getLeaderURL(ctx, client.GetClientConn())
				if err != nil {
					simutil.Logger.Error("[heartbeatStreamLoop] failed to get leader URL", zap.Error(err))
					continue
				}
				if err = c.changeConn(conn); err == nil {
					break
				}
			}
		}
		simutil.Logger.Info("recreate heartbeat stream", zap.String("tag", c.tag))
	}
}

func (c *client) receiveRegionHeartbeat(ctx context.Context, stream pdpb.PD_RegionHeartbeatClient, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		resp, err := stream.Recv()
		if err != nil {
			errCh <- err
			simutil.Logger.Error("receive regionHeartbeat error", zap.String("tag", c.tag), zap.Error(err))
			return
		}
		select {
		case c.receiveRegionHeartbeatCh <- resp:
		case <-ctx.Done():
			return
		}
	}
}

func (c *client) reportRegionHeartbeat(ctx context.Context, stream pdpb.PD_RegionHeartbeatClient, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case region := <-c.reportRegionHeartbeatCh:
			if region == nil {
				simutil.Logger.Error("report nil regionHeartbeat error",
					zap.String("tag", c.tag), zap.Error(errors.New("nil region")))
			}
			request := &pdpb.RegionHeartbeatRequest{
				Header:          requestHeader(),
				Region:          region.GetMeta(),
				Leader:          region.GetLeader(),
				DownPeers:       region.GetDownPeers(),
				PendingPeers:    region.GetPendingPeers(),
				BytesWritten:    region.GetBytesWritten(),
				BytesRead:       region.GetBytesRead(),
				ApproximateSize: uint64(region.GetApproximateSize()),
				ApproximateKeys: uint64(region.GetApproximateKeys()),
			}
			err := stream.Send(request)
			if err != nil {
				errCh <- err
				simutil.Logger.Error("report regionHeartbeat error", zap.String("tag", c.tag), zap.Error(err))
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// Close closes the client.
func (c *client) Close() {
	if c.cancel == nil {
		simutil.Logger.Info("pd client has been closed", zap.String("tag", c.tag))
		return
	}
	simutil.Logger.Info("closing pd client", zap.String("tag", c.tag))
	c.cancel()
	c.wg.Wait()

	if err := c.clientConn.Close(); err != nil {
		simutil.Logger.Error("failed to close grpc client connection", zap.String("tag", c.tag), zap.Error(err))
	}
}

func (c *client) allocID(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().AllocID(ctx, &pdpb.AllocIDRequest{
		Header: requestHeader(),
	})
	cancel()
	if err != nil {
		return 0, err
	}
	if resp.GetHeader().GetError() != nil {
		return 0, errors.Errorf("alloc id failed: %s", resp.GetHeader().GetError().String())
	}
	return resp.GetId(), nil
}

// PutStore sends PutStore to PD.
func (c *client) PutStore(ctx context.Context, store *metapb.Store) error {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	newStore := typeutil.DeepClone(store, core.StoreFactory)
	resp, err := c.pdClient().PutStore(ctx, &pdpb.PutStoreRequest{
		Header: requestHeader(),
		Store:  newStore,
	})
	cancel()
	if err != nil {
		return err
	}
	if resp.Header.GetError() != nil {
		simutil.Logger.Error("put store error", zap.Reflect("error", resp.Header.GetError()))
		return nil
	}
	return nil
}

// StoreHeartbeat sends a StoreHeartbeat to PD.
func (c *client) StoreHeartbeat(ctx context.Context, newStats *pdpb.StoreStats) error {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().StoreHeartbeat(ctx, &pdpb.StoreHeartbeatRequest{
		Header: requestHeader(),
		Stats:  newStats,
	})
	cancel()
	if err != nil {
		return err
	}
	if resp.Header.GetError() != nil {
		simutil.Logger.Error("store heartbeat error", zap.Reflect("error", resp.Header.GetError()))
		return nil
	}
	return nil
}

// RegionHeartbeat sends a RegionHeartbeat to PD.
func (c *client) RegionHeartbeat(_ context.Context, region *core.RegionInfo) error {
	c.reportRegionHeartbeatCh <- region
	return nil
}

type retryClient struct {
	client     Client
	retryCount int
}

func newRetryClient(node *Node) *retryClient {
	// Init PD client and putting it into node.
	tag := fmt.Sprintf("store %d", node.Store.Id)
	var (
		client                   Client
		receiveRegionHeartbeatCh <-chan *pdpb.RegionHeartbeatResponse
		err                      error
	)

	// Client should wait if PD server is not ready.
	for range maxInitClusterRetries {
		client, receiveRegionHeartbeatCh, err = NewClient(tag)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	if err != nil {
		simutil.Logger.Fatal("create client failed", zap.Error(err))
	}
	node.client = client

	// Init retryClient
	retryClient := &retryClient{
		client:     client,
		retryCount: retryTimes,
	}
	// check leader url firstly
	retryClient.requestWithRetry(func() (any, error) {
		return nil, errors.New("retry to create client")
	})
	// start heartbeat stream
	node.receiveRegionHeartbeatCh = receiveRegionHeartbeatCh
	go client.heartbeatStreamLoop()

	return retryClient
}

func (rc *retryClient) requestWithRetry(f func() (any, error)) (any, error) {
	// execute the function directly
	if res, err := f(); err == nil {
		return res, nil
	}
	// retry to get leader URL
	for range rc.retryCount {
		SD.ScheduleCheckMemberChanged()
		time.Sleep(100 * time.Millisecond)
		if client := SD.GetServiceClient(); client != nil {
			_, conn, err := getLeaderURL(context.Background(), client.GetClientConn())
			if err != nil {
				simutil.Logger.Error("[retry] failed to get leader URL", zap.Error(err))
				return nil, err
			}
			if err = rc.client.changeConn(conn); err != nil {
				simutil.Logger.Error("failed to change connection", zap.Error(err))
				return nil, err
			}
			return f()
		}
	}
	return nil, errors.New("failed to retry")
}

func getLeaderURL(ctx context.Context, conn *grpc.ClientConn) (string, *grpc.ClientConn, error) {
	pdCli := pdpb.NewPDClient(conn)
	members, err := pdCli.GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		return "", nil, err
	}
	if members.GetHeader().GetError() != nil {
		return "", nil, errors.New(members.GetHeader().GetError().String())
	}
	clusterID.Store(members.GetHeader().GetClusterId())
	if clusterID.Load() == 0 {
		return "", nil, errors.New("cluster id is 0")
	}
	if members.GetLeader() == nil {
		return "", nil, errors.New("leader is nil")
	}
	leaderURL := members.GetLeader().ClientUrls[0]
	conn, err = createConn(leaderURL)
	return leaderURL, conn, err
}

func (rc *retryClient) allocID(ctx context.Context) (uint64, error) {
	res, err := rc.requestWithRetry(func() (any, error) {
		id, err := rc.client.allocID(ctx)
		return id, err
	})
	if err != nil {
		return 0, err
	}
	return res.(uint64), nil
}

// PutStore sends PutStore to PD.
func (rc *retryClient) PutStore(ctx context.Context, store *metapb.Store) error {
	_, err := rc.requestWithRetry(func() (any, error) {
		err := rc.client.PutStore(ctx, store)
		return nil, err
	})
	return err
}

// StoreHeartbeat sends a StoreHeartbeat to PD.
func (rc *retryClient) StoreHeartbeat(ctx context.Context, newStats *pdpb.StoreStats) error {
	_, err := rc.requestWithRetry(func() (any, error) {
		err := rc.client.StoreHeartbeat(ctx, newStats)
		return nil, err
	})
	return err
}

// RegionHeartbeat sends a RegionHeartbeat to PD.
func (rc *retryClient) RegionHeartbeat(ctx context.Context, region *core.RegionInfo) error {
	_, err := rc.requestWithRetry(func() (any, error) {
		err := rc.client.RegionHeartbeat(ctx, region)
		return nil, err
	})
	return err
}

func (*retryClient) changeConn(_ *grpc.ClientConn) error {
	panic("unImplement")
}

func (rc *retryClient) heartbeatStreamLoop() {
	rc.client.heartbeatStreamLoop()
}

// Close closes the client.
func (rc *retryClient) Close() {
	rc.client.Close()
}

// Bootstrap bootstraps the cluster and using the given PD address firstly.
// because before bootstrapping the cluster, PDServiceDiscovery can not been started.
func Bootstrap(ctx context.Context, pdAddrs string, store *metapb.Store, region *metapb.Region) (
	leaderURL string, pdCli pdpb.PDClient, err error) {
	urls := strings.Split(pdAddrs, ",")
	if len(urls) == 0 {
		return "", nil, errors.New("empty pd address")
	}

retry:
	for range maxInitClusterRetries {
		time.Sleep(100 * time.Millisecond)
		for _, url := range urls {
			conn, err := createConn(url)
			if err != nil {
				continue
			}
			leaderURL, conn, err = getLeaderURL(ctx, conn)
			if err != nil {
				continue
			}
			pdCli = pdpb.NewPDClient(conn)
			break retry
		}
	}
	if clusterID.Load() == 0 {
		return "", nil, errors.WithStack(errFailInitClusterID)
	}
	simutil.Logger.Info("get cluster id successfully", zap.Uint64("cluster-id", clusterID.Load()))

	// Check if the cluster is already bootstrapped.
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	defer cancel()
	req := &pdpb.IsBootstrappedRequest{
		Header: requestHeader(),
	}
	resp, err := pdCli.IsBootstrapped(ctx, req)
	if resp.GetBootstrapped() {
		simutil.Logger.Fatal("failed to bootstrap, server is not clean")
	}
	if err != nil {
		return "", nil, err
	}
	// Bootstrap the cluster.
	newStore := typeutil.DeepClone(store, core.StoreFactory)
	newRegion := typeutil.DeepClone(region, core.RegionFactory)
	var res *pdpb.BootstrapResponse
	for range maxInitClusterRetries {
		// Bootstrap the cluster.
		res, err = pdCli.Bootstrap(ctx, &pdpb.BootstrapRequest{
			Header: requestHeader(),
			Store:  newStore,
			Region: newRegion,
		})
		if err != nil {
			continue
		}
		if res.GetHeader().GetError() != nil {
			continue
		}
		break
	}
	if err != nil {
		return "", nil, err
	}
	if res.GetHeader().GetError() != nil {
		return "", nil, errors.New(res.GetHeader().GetError().String())
	}

	return leaderURL, pdCli, nil
}

/* PDHTTPClient is a client for PD HTTP API, these are the functions that are used in the simulator */
func PutPDConfig(config *sc.PDConfig) error {
	if len(config.PlacementRules) > 0 {
		ruleOps := make([]*pdHttp.RuleOp, 0)
		for _, rule := range config.PlacementRules {
			ruleOps = append(ruleOps, &pdHttp.RuleOp{
				Rule:   rule,
				Action: pdHttp.RuleOpAdd,
			})
		}
		err := PDHTTPClient.SetPlacementRuleInBatch(context.Background(), ruleOps)
		if err != nil {
			return err
		}
		simutil.Logger.Info("add placement rule success", zap.Any("rules", config.PlacementRules))
	}
	if len(config.LocationLabels) > 0 {
		data := make(map[string]any)
		data["location-labels"] = config.LocationLabels
		err := PDHTTPClient.SetConfig(context.Background(), data)
		if err != nil {
			return err
		}
		simutil.Logger.Info("add location labels success", zap.Any("labels", config.LocationLabels))
	}
	return nil
}

// ChooseToHaltPDSchedule is used to choose whether to halt the PD schedule
func ChooseToHaltPDSchedule(halt bool) {
	haltSchedule.Store(halt)
	PDHTTPClient.SetConfig(context.Background(), map[string]any{
		"schedule.halt-scheduling": strconv.FormatBool(halt),
	})
}
