// Copyright 2024 TiKV Project Authors.
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

package heartbeatbench

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	pdHttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/tools/pd-dev/util"
	"go.etcd.io/etcd/pkg/report"
	"go.uber.org/zap"
)

const (
	bytesUnit            = 128
	keysUint             = 8
	queryUnit            = 8
	hotByteUnit          = 16 * units.KiB
	hotKeysUint          = 256
	hotQueryUnit         = 256
	regionReportInterval = 60 // 60s
	storeReportInterval  = 10 // 10s
	capacity             = 4 * units.TiB
)

var (
	clusterID  uint64
	maxVersion uint64 = 1
)

func newClient(ctx context.Context, cfg *config) (pdpb.PDClient, error) {
	tlsConfig := util.LoadTLSConfig(cfg.GeneralConfig)
	cc, err := grpcutil.GetClientConn(ctx, cfg.PDAddrs, tlsConfig)
	if err != nil {
		return nil, err
	}
	return pdpb.NewPDClient(cc), nil
}

func initClusterID(ctx context.Context, cli pdpb.PDClient) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cctx, cancel := context.WithCancel(ctx)
			res, err := cli.GetMembers(cctx, &pdpb.GetMembersRequest{})
			cancel()
			if err != nil {
				continue
			}
			if res.GetHeader().GetError() != nil {
				continue
			}
			clusterID = res.GetHeader().GetClusterId()
			log.Info("init cluster ID successfully", zap.Uint64("cluster-id", clusterID))
			return
		}
	}
}

func header() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
}

func bootstrap(ctx context.Context, cli pdpb.PDClient) {
	cctx, cancel := context.WithCancel(ctx)
	isBootstrapped, err := cli.IsBootstrapped(cctx, &pdpb.IsBootstrappedRequest{Header: header()})
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
		Version: "8.0.0-alpha",
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
	cctx, cancel = context.WithCancel(ctx)
	resp, err := cli.Bootstrap(cctx, req)
	cancel()
	if err != nil {
		log.Fatal("failed to bootstrap the cluster", zap.Error(err))
	}
	if resp.GetHeader().GetError() != nil {
		log.Fatal("failed to bootstrap the cluster", zap.String("err", resp.GetHeader().GetError().String()))
	}
	log.Info("bootstrapped")
}

func putStores(ctx context.Context, cfg *config, cli pdpb.PDClient, stores *Stores) {
	for i := uint64(1); i <= uint64(cfg.StoreCount); i++ {
		store := &metapb.Store{
			Id:      i,
			Address: fmt.Sprintf("localhost:%d", i),
			Version: "8.0.0-alpha",
		}
		cctx, cancel := context.WithCancel(ctx)
		resp, err := cli.PutStore(cctx, &pdpb.PutStoreRequest{Header: header(), Store: store})
		cancel()
		if err != nil {
			log.Fatal("failed to put store", zap.Uint64("store-id", i), zap.Error(err))
		}
		if resp.GetHeader().GetError() != nil {
			log.Fatal("failed to put store", zap.Uint64("store-id", i), zap.String("err", resp.GetHeader().GetError().String()))
		}
		go func(ctx context.Context, storeID uint64) {
			var heartbeatTicker = time.NewTicker(10 * time.Second)
			defer heartbeatTicker.Stop()
			for {
				select {
				case <-heartbeatTicker.C:
					stores.heartbeat(ctx, cli, storeID)
				case <-ctx.Done():
					return
				}
			}
		}(ctx, i)
	}
}

func deleteOperators(ctx context.Context, httpCli pdHttp.Client) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := httpCli.DeleteOperators(ctx)
			if err != nil {
				log.Error("fail to delete operators", zap.Error(err))
			}
		}
	}
}

func newReport(cfg *config) report.Report {
	p := "%4.4f"
	if cfg.Sample {
		return report.NewReportSample(p)
	}
	return report.NewReport(p)
}

func randomPick(slice []int, total int, ratio float64) []int {
	rand.Shuffle(total, func(i, j int) {
		slice[i], slice[j] = slice[j], slice[i]
	})
	return append(slice[:0:0], slice[0:int(float64(total)*ratio)]...)
}

func pick(slice []int, total int, ratio float64) []int {
	return append(slice[:0:0], slice[0:int(float64(total)*ratio)]...)
}
