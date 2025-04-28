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
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
<<<<<<< HEAD
	"github.com/pkg/errors"
=======
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	etcdtypes "go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
>>>>>>> ac5967544 (Metrics: Go runtime metrics (#8927))
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
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
<<<<<<< HEAD
func PromHandler(handler http.Handler) gin.HandlerFunc {
=======
func PromHandler() gin.HandlerFunc {
	prometheus.DefaultRegisterer.Unregister(collectors.NewGoCollector())
	if err := prometheus.Register(collectors.NewGoCollector(collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsGC, collectors.MetricsMemory, collectors.MetricsScheduler))); err != nil {
		log.Warn("go runtime collectors have already registered", errs.ZapError(err))
	}
>>>>>>> ac5967544 (Metrics: Go runtime metrics (#8927))
	return func(c *gin.Context) {
		handler.ServeHTTP(c.Writer, c.Request)
	}
}
