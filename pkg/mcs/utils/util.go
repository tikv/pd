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
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	// maxRetryTimes is the max retry times for initializing the cluster ID.
	maxRetryTimes = 5
	// ClusterIDPath is the path to store cluster id
	ClusterIDPath = "/pd/cluster_id"
	// retryInterval is the interval to retry.
	retryInterval = time.Second
)

// InitClusterID initializes the cluster ID.
func InitClusterID(ctx context.Context, client *clientv3.Client) (id uint64, err error) {
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	for i := 0; i < maxRetryTimes; i++ {
		if clusterID, err := etcdutil.GetClusterID(client, ClusterIDPath); err == nil && clusterID != 0 {
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
	GetDelegateClient(ctx context.Context, tlsCfg *grpcutil.TLSConfig, forwardedHost string) (*grpc.ClientConn, error)
	ServerLoopWgDone()
	ServerLoopWgAdd(int)
	IsClosed() bool
	GetHTTPServer() *http.Server
	GetGRPCServer() *grpc.Server
	SetGRPCServer(*grpc.Server)
	SetHTTPServer(*http.Server)
	SetETCDClient(*clientv3.Client)
	SetHTTPClient(*http.Client)
	IsSecure() bool
	RegisterGRPCService(*grpc.Server)
	SetUpRestHandler() (http.Handler, apiutil.APIServiceGroup)
	diagnosticspb.DiagnosticsServer
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
	cc, err := s.GetDelegateClient(s.Context(), s.GetTLSConfig(), urls[0])
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

// InitClient initializes the etcd and http clients.
func InitClient(s server) error {
	tlsConfig, err := s.GetTLSConfig().ToTLSConfig()
	if err != nil {
		return err
	}
	backendUrls, err := types.NewURLs(strings.Split(s.GetBackendEndpoints(), ","))
	if err != nil {
		return err
	}
	etcdClient, httpClient, err := etcdutil.CreateClients(tlsConfig, backendUrls)
	if err != nil {
		return err
	}
	s.SetETCDClient(etcdClient)
	s.SetHTTPClient(httpClient)
	return nil
}

func startGRPCServer(s server, l net.Listener) {
	defer logutil.LogPanic()
	defer s.ServerLoopWgDone()

	log.Info("grpc server starts serving", zap.String("address", l.Addr().String()))
	err := s.GetGRPCServer().Serve(l)
	if s.IsClosed() {
		log.Info("grpc server stopped")
	} else {
		log.Fatal("grpc server stopped unexpectedly", errs.ZapError(err))
	}
}

func startHTTPServer(s server, l net.Listener) {
	defer logutil.LogPanic()
	defer s.ServerLoopWgDone()

	log.Info("http server starts serving", zap.String("address", l.Addr().String()))
	err := s.GetHTTPServer().Serve(l)
	if s.IsClosed() {
		log.Info("http server stopped")
	} else {
		log.Fatal("http server stopped unexpectedly", errs.ZapError(err))
	}
}

// StartGRPCAndHTTPServers starts the grpc and http servers.
func StartGRPCAndHTTPServers(s server, serverReadyChan chan<- struct{}, l net.Listener) {
	defer logutil.LogPanic()
	defer s.ServerLoopWgDone()

	mux := cmux.New(l)
	// Don't hang on matcher after closing listener
	mux.SetReadTimeout(3 * time.Second)
	grpcL := mux.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	var httpListener net.Listener
	if s.IsSecure() {
		httpListener = mux.Match(cmux.Any())
	} else {
		httpListener = mux.Match(cmux.HTTP1())
	}

	grpcServer := grpc.NewServer()
	s.SetGRPCServer(grpcServer)
	s.RegisterGRPCService(grpcServer)
	diagnosticspb.RegisterDiagnosticsServer(grpcServer, s)
	s.ServerLoopWgAdd(1)
	go startGRPCServer(s, grpcL)

	handler, _ := s.SetUpRestHandler()
	s.SetHTTPServer(&http.Server{
		Handler:     handler,
		ReadTimeout: 3 * time.Second,
	})
	s.ServerLoopWgAdd(1)
	go startHTTPServer(s, httpListener)

	serverReadyChan <- struct{}{}
	if err := mux.Serve(); err != nil {
		if s.IsClosed() {
			log.Info("mux stopped serving", errs.ZapError(err))
		} else {
			log.Fatal("mux stopped serving unexpectedly", errs.ZapError(err))
		}
	}
}

// StopHTTPServer stops the http server.
func StopHTTPServer(s server) {
	log.Info("stopping http server")
	defer log.Info("http server stopped")

	ctx, cancel := context.WithTimeout(context.Background(), DefaultHTTPGracefulShutdownTimeout)
	defer cancel()

	// First, try to gracefully shutdown the http server
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		s.GetHTTPServer().Shutdown(ctx)
	}()

	select {
	case <-ch:
	case <-ctx.Done():
		// Took too long, manually close open transports
		log.Warn("http server graceful shutdown timeout, forcing close")
		s.GetHTTPServer().Close()
		// concurrent Graceful Shutdown should be interrupted
		<-ch
	}
}

// StopGRPCServer stops the grpc server.
func StopGRPCServer(s server) {
	log.Info("stopping grpc server")
	defer log.Info("grpc server stopped")

	// Do not grpc.Server.GracefulStop with TLS enabled etcd server
	// See https://github.com/grpc/grpc-go/issues/1384#issuecomment-317124531
	// and https://github.com/etcd-io/etcd/issues/8916
	if s.IsSecure() {
		s.GetGRPCServer().Stop()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultGRPCGracefulStopTimeout)
	defer cancel()

	// First, try to gracefully shutdown the grpc server
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		// Close listeners to stop accepting new connections,
		// will block on any existing transports
		s.GetGRPCServer().GracefulStop()
	}()

	// Wait until all pending RPCs are finished
	select {
	case <-ch:
	case <-ctx.Done():
		// Took too long, manually close open transports
		// e.g. watch streams
		log.Warn("grpc server graceful shutdown timeout, forcing close")
		s.GetGRPCServer().Stop()
		// concurrent GracefulStop should be interrupted
		<-ch
	}
}

// Exit exits the program with the given code.
func Exit(code int) {
	log.Sync()
	os.Exit(code)
}
