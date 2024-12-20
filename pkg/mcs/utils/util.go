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
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
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
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

// PromHandler is a handler to get prometheus metrics.
func PromHandler() gin.HandlerFunc {
	prometheus.DefaultRegisterer.Unregister(collectors.NewGoCollector())
	if err := prometheus.Register(collectors.NewGoCollector(collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsGC, collectors.MetricsMemory, collectors.MetricsScheduler))); err != nil {
		log.Warn("go runtime collectors have already registered", errs.ZapError(err))
	}
	return func(c *gin.Context) {
		// register promhttp.HandlerOpts DisableCompression
		promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer, promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
			DisableCompression: true,
		})).ServeHTTP(c.Writer, c.Request)
	}
}

// StatusHandler is a handler to get status info.
func StatusHandler(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(server)
	version := versioninfo.Status{
		BuildTS:        versioninfo.PDBuildTS,
		GitHash:        versioninfo.PDGitHash,
		Version:        versioninfo.PDReleaseVersion,
		StartTimestamp: svr.StartTimestamp(),
	}

	c.IndentedJSON(http.StatusOK, version)
}

type server interface {
	GetAdvertiseListenAddr() string
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
	GetEtcdClient() *clientv3.Client
	SetEtcdClient(*clientv3.Client)
	SetHTTPClient(*http.Client)
	IsSecure() bool
	RegisterGRPCService(*grpc.Server)
	SetUpRestHandler() (http.Handler, apiutil.APIServiceGroup)
	diagnosticspb.DiagnosticsServer
	StartTimestamp() int64
	Name() string
}

// InitClient initializes the etcd and http clients.
func InitClient(s server) error {
	tlsConfig, err := s.GetTLSConfig().ToTLSConfig()
	if err != nil {
		return err
	}
	backendUrls, err := etcdtypes.NewURLs(strings.Split(s.GetBackendEndpoints(), ","))
	if err != nil {
		return err
	}
	etcdClient, err := etcdutil.CreateEtcdClient(tlsConfig, backendUrls, "mcs-etcd-client")
	if err != nil {
		return err
	}
	s.SetEtcdClient(etcdClient)
	s.SetHTTPClient(etcdutil.CreateHTTPClient(tlsConfig))
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

	grpcServer := grpc.NewServer(
		// Allow clients send consecutive pings in every 5 seconds.
		// The default value of MinTime is 5 minutes,
		// which is too long compared with 10 seconds of TiKV's pd client keepalive time.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime: 5 * time.Second,
		}),
	)
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

	ctx, cancel := context.WithTimeout(context.Background(), constant.DefaultHTTPGracefulShutdownTimeout)
	defer cancel()

	// First, try to gracefully shutdown the http server
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		if err := s.GetHTTPServer().Shutdown(ctx); err != nil {
			log.Error("http server graceful shutdown failed", errs.ZapError(err))
		}
	}()

	select {
	case <-ch:
	case <-ctx.Done():
		// Took too long, manually close open transports
		log.Warn("http server graceful shutdown timeout, forcing close")
		if err := s.GetHTTPServer().Close(); err != nil {
			log.Warn("http server close failed", errs.ZapError(err))
		}
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

	ctx, cancel := context.WithTimeout(context.Background(), constant.DefaultGRPCGracefulStopTimeout)
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

// Register registers the service.
func Register(s server, serviceName string) (*discovery.ServiceRegistryEntry, *discovery.ServiceRegister, error) {
	if err := endpoint.InitClusterIDForMs(s.Context(), s.GetEtcdClient()); err != nil {
		return nil, nil, err
	}
	execPath, err := os.Executable()
	deployPath := filepath.Dir(execPath)
	if err != nil {
		deployPath = ""
	}
	serviceID := &discovery.ServiceRegistryEntry{
		ServiceAddr:    s.GetAdvertiseListenAddr(),
		Version:        versioninfo.PDReleaseVersion,
		GitHash:        versioninfo.PDGitHash,
		DeployPath:     deployPath,
		StartTimestamp: s.StartTimestamp(),
		Name:           s.Name(),
	}
	serializedEntry, err := serviceID.Serialize()
	if err != nil {
		return nil, nil, err
	}
	serviceRegister := discovery.NewServiceRegister(s.Context(), s.GetEtcdClient(),
		serviceName, s.GetAdvertiseListenAddr(), serializedEntry,
		discovery.DefaultLeaseInSeconds)
	if err := serviceRegister.Register(); err != nil {
		log.Error("failed to register the service", zap.String("service-name", serviceName), errs.ZapError(err))
		return nil, nil, err
	}
	return serviceID, serviceRegister, nil
}

// Exit exits the program with the given code.
func Exit(code int) {
	log.Sync()
	os.Exit(code)
}
