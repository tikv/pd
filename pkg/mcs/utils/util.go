// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	// maxRetryTimes is the max retry times for initializing the cluster ID.
	maxRetryTimes = 5
	// clusterIDPath is the path to store cluster id
	clusterIDPath = "/pd/cluster_id"
	// retryInterval is the interval to retry.
	retryInterval = time.Second
)

// InitClusterID initializes the cluster ID.
func InitClusterID(ctx context.Context, client *clientv3.Client) (id uint64, err error) {
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	for i := 0; i < maxRetryTimes; i++ {
		if clusterID, err := etcdutil.GetClusterID(client, clusterIDPath); err == nil && clusterID != 0 {
			return clusterID, nil
		}
		select {
		case <-ctx.Done():
			return 0, err
		case <-ticker.C:
		}
	}
	return 0, errors.Errorf("failed to init cluster ID after retrying %d times", maxRetryTimes)
}

// PromHandler is a handler to get prometheus metrics.
func PromHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		// register promhttp.HandlerOpts DisableCompression
		promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer, promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
			DisableCompression: true,
		})).ServeHTTP(c.Writer, c.Request)
	}
}

type server interface {
	GetBackendEndpoints() string
	Context() context.Context
	GetTLSConfig() *grpcutil.TLSConfig
	GetClientConns() *sync.Map
	GetDelegateClient(ctx context.Context, forwardedHost string) (*grpc.ClientConn, error)
}

// WaitAPIServiceReady waits for the api service ready.
func WaitAPIServiceReady(s server) error {
	var (
		ready bool
		err   error
	)
	ticker := time.NewTicker(RetryIntervalWaitAPIService)
	defer ticker.Stop()
	for i := 0; i < MaxRetryTimesWaitAPIService; i++ {
		ready, err = isAPIServiceReady(s)
		if err == nil && ready {
			return nil
		}
		log.Debug("api server is not ready, retrying", errs.ZapError(err), zap.Bool("ready", ready))
		select {
		case <-s.Context().Done():
			return errors.New("context canceled while waiting api server ready")
		case <-ticker.C:
		}
	}
	if err != nil {
		log.Warn("failed to check api server ready", errs.ZapError(err))
	}
	return errors.Errorf("failed to wait api server ready after retrying %d times", MaxRetryTimesWaitAPIService)
}

func isAPIServiceReady(s server) (bool, error) {
	urls := strings.Split(s.GetBackendEndpoints(), ",")
	if len(urls) == 0 {
		return false, errors.New("no backend endpoints")
	}
	cc, err := s.GetDelegateClient(s.Context(), urls[0])
	if err != nil {
		return false, err
	}
	clusterInfo, err := pdpb.NewPDClient(cc).GetClusterInfo(s.Context(), &pdpb.GetClusterInfoRequest{})
	if err != nil {
		return false, err
	}
	if clusterInfo.GetHeader().GetError() != nil {
		return false, errors.Errorf(clusterInfo.GetHeader().GetError().String())
	}
	modes := clusterInfo.ServiceModes
	if len(modes) == 0 {
		return false, errors.New("no service mode")
	}
	if modes[0] == pdpb.ServiceMode_API_SVC_MODE {
		return true, nil
	}
	return false, nil
}
