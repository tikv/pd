// Copyright 2023 TiKV Project Authors.
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

package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/pingcap/sysutil"
	"github.com/spf13/cobra"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/systimemon"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ bs.Server = (*Server)(nil)
var _ tso.ElectionMember = (*member.Participant)(nil)

const serviceName = "TSO Service"

// Server is the TSO server, and it implements bs.Server.
type Server struct {
	*server.BaseServer
	diagnosticspb.DiagnosticsServer

	// Server state. 0 is not running, 1 is running.
	isRunning int64

	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	cfg *Config

	service              *Service
	keyspaceGroupManager *tso.KeyspaceGroupManager

	// tsoProtoFactory is the abstract factory for creating tso
	// related data structures defined in the tso grpc protocol
	tsoProtoFactory *tsoutil.TSOProtoFactory

	// for service registry
	serviceID       *discovery.ServiceRegistryEntry
	serviceRegister *discovery.ServiceRegister
}

// Implement the following methods defined in bs.Server

// Name returns the unique name for this server in the TSO cluster.
func (s *Server) Name() string {
	return s.cfg.Name
}

// GetBasicServer returns the basic server.
func (s *Server) GetBasicServer() bs.Server {
	return s
}

// GetAddr returns the address of the server.
func (s *Server) GetAddr() string {
	return s.cfg.ListenAddr
}

// GetAdvertiseListenAddr returns the advertise address of the server.
func (s *Server) GetAdvertiseListenAddr() string {
	return s.cfg.AdvertiseListenAddr
}

// GetBackendEndpoints returns the backend endpoints.
func (s *Server) GetBackendEndpoints() string {
	return s.cfg.BackendEndpoints
}

// ServerLoopWgDone decreases the server loop wait group.
func (s *Server) ServerLoopWgDone() {
	s.serverLoopWg.Done()
}

// ServerLoopWgAdd increases the server loop wait group.
func (s *Server) ServerLoopWgAdd(n int) {
	s.serverLoopWg.Add(n)
}

// SetUpRestHandler sets up the REST handler.
func (s *Server) SetUpRestHandler() (http.Handler, apiutil.APIServiceGroup) {
	return SetUpRestHandler(s.service)
}

// RegisterGRPCService registers the grpc service.
func (s *Server) RegisterGRPCService(grpcServer *grpc.Server) {
	s.service.RegisterGRPCService(grpcServer)
}

// SetLogLevel sets log level.
func (s *Server) SetLogLevel(level string) error {
	if !logutil.IsLevelLegal(level) {
		return errors.Errorf("log level %s is illegal", level)
	}
	s.cfg.Log.Level = level
	log.SetLevel(logutil.StringToZapLogLevel(level))
	log.Warn("log level changed", zap.String("level", log.GetLevel().String()))
	return nil
}

// Run runs the TSO server.
func (s *Server) Run() (err error) {
	go systimemon.StartMonitor(s.Context(), time.Now, func() {
		log.Error("system time jumps backward", errs.ZapError(errs.ErrIncorrectSystemTime))
		timeJumpBackCounter.Inc()
	})

	if err = utils.InitClient(s); err != nil {
		return err
	}

	if s.serviceID, s.serviceRegister, err = utils.Register(s, constant.TSOServiceName); err != nil {
		return err
	}

	return s.startServer()
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.isRunning, 1, 0) {
		// server is already closed
		return
	}

	log.Info("closing tso server ...")
	// close tso service loops in the keyspace group manager
	s.keyspaceGroupManager.Close()
	if err := s.serviceRegister.Deregister(); err != nil {
		log.Error("failed to deregister the service", errs.ZapError(err))
	}
	utils.StopHTTPServer(s)
	utils.StopGRPCServer(s)
	s.GetListener().Close()
	s.CloseClientConns()
	s.serverLoopCancel()
	s.serverLoopWg.Wait()

	if s.GetClient() != nil {
		if err := s.GetClient().Close(); err != nil {
			log.Error("close etcd client meet error", errs.ZapError(errs.ErrCloseEtcdClient, err))
		}
	}

	if s.GetHTTPClient() != nil {
		s.GetHTTPClient().CloseIdleConnections()
	}
	log.Info("tso server is closed")
}

// IsServing implements basicserver. It returns whether the server is the leader
// if there is embedded etcd, or the primary otherwise.
func (s *Server) IsServing() bool {
	return s.IsKeyspaceServing(constant.DefaultKeyspaceID, constant.DefaultKeyspaceGroupID)
}

// IsKeyspaceServing returns whether the server is the primary of the given keyspace.
// TODO: update basicserver interface to support keyspace.
func (s *Server) IsKeyspaceServing(keyspaceID, keyspaceGroupID uint32) bool {
	if atomic.LoadInt64(&s.isRunning) == 0 {
		return false
	}

	member, err := s.keyspaceGroupManager.GetElectionMember(
		keyspaceID, keyspaceGroupID)
	if err != nil {
		log.Error("failed to get election member", errs.ZapError(err))
		return false
	}
	return member.IsLeader()
}

// GetLeaderListenUrls gets service endpoints from the leader in election group.
// The entry at the index 0 is the primary's service endpoint.
func (s *Server) GetLeaderListenUrls() []string {
	member, err := s.keyspaceGroupManager.GetElectionMember(
		constant.DefaultKeyspaceID, constant.DefaultKeyspaceGroupID)
	if err != nil {
		log.Error("failed to get election member", errs.ZapError(err))
		return nil
	}

	return member.GetLeaderListenUrls()
}

// GetMember returns the election member of the given keyspace and keyspace group.
func (s *Server) GetMember(keyspaceID, keyspaceGroupID uint32) (tso.ElectionMember, error) {
	member, err := s.keyspaceGroupManager.GetElectionMember(keyspaceID, keyspaceGroupID)
	if err != nil {
		return nil, err
	}
	return member, nil
}

// ResignPrimary resigns the primary of the given keyspace.
func (s *Server) ResignPrimary(keyspaceID, keyspaceGroupID uint32) error {
	member, err := s.keyspaceGroupManager.GetElectionMember(keyspaceID, keyspaceGroupID)
	if err != nil {
		return err
	}
	member.ResetLeader()
	return nil
}

// AddServiceReadyCallback implements basicserver.
// It adds callbacks when it's ready for providing tso service.
func (*Server) AddServiceReadyCallback(...func(context.Context) error) {
	// Do nothing here. The primary of each keyspace group assigned to this host
	// will respond to the requests accordingly.
}

// Implement the other methods

// IsClosed checks if the server loop is closed
func (s *Server) IsClosed() bool {
	return atomic.LoadInt64(&s.isRunning) == 0
}

// GetKeyspaceGroupManager returns the manager of keyspace group.
func (s *Server) GetKeyspaceGroupManager() *tso.KeyspaceGroupManager {
	return s.keyspaceGroupManager
}

// GetTSOAllocatorManager returns the manager of TSO Allocator.
func (s *Server) GetTSOAllocatorManager(keyspaceGroupID uint32) (*tso.AllocatorManager, error) {
	return s.keyspaceGroupManager.GetAllocatorManager(keyspaceGroupID)
}

// IsLocalRequest checks if the forwarded host is the current host
func (*Server) IsLocalRequest(forwardedHost string) bool {
	// TODO: Check if the forwarded host is the current host.
	// The logic is depending on etcd service mode -- if the TSO service
	// uses the embedded etcd, check against ClientUrls; otherwise check
	// against the cluster membership.
	return forwardedHost == ""
}

// ValidateInternalRequest checks if server is closed, which is used to validate
// the gRPC communication between TSO servers internally.
// TODO: Check if the sender is from the global TSO allocator
func (s *Server) ValidateInternalRequest(_ *tsopb.RequestHeader, _ bool) error {
	if s.IsClosed() {
		return ErrNotStarted
	}
	return nil
}

// ValidateRequest checks if the keyspace replica is the primary and clusterID is matched.
// TODO: Check if the keyspace replica is the primary
func (s *Server) ValidateRequest(header *tsopb.RequestHeader) error {
	if s.IsClosed() {
		return ErrNotStarted
	}
	if header.GetClusterId() != keypath.ClusterID() {
		return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d",
			keypath.ClusterID(), header.GetClusterId())
	}
	return nil
}

// GetExternalTS returns external timestamp from the cache or the persistent storage.
// TODO: Implement GetExternalTS
func (*Server) GetExternalTS() uint64 {
	return 0
}

// SetExternalTS saves external timestamp to cache and the persistent storage.
// TODO: Implement SetExternalTS
func (*Server) SetExternalTS(uint64) error {
	return nil
}

// ResetTS resets the TSO with the specified one.
func (s *Server) ResetTS(ts uint64, ignoreSmaller, skipUpperBoundCheck bool, keyspaceGroupID uint32) error {
	log.Info("reset-ts",
		zap.Uint64("new-ts", ts),
		zap.Bool("ignore-smaller", ignoreSmaller),
		zap.Bool("skip-upper-bound-check", skipUpperBoundCheck),
		zap.Uint32("keyspace-group-id", keyspaceGroupID))
	tsoAllocatorManager, err := s.GetTSOAllocatorManager(keyspaceGroupID)
	if err != nil {
		log.Error("failed to get allocator manager", errs.ZapError(err))
		return err
	}
	tsoAllocator, err := tsoAllocatorManager.GetAllocator(tso.GlobalDCLocation)
	if err != nil {
		return err
	}
	if tsoAllocator == nil {
		return errs.ErrServerNotStarted
	}
	return tsoAllocator.SetTSO(ts, ignoreSmaller, skipUpperBoundCheck)
}

// GetConfig gets the config.
func (s *Server) GetConfig() *Config {
	return s.cfg
}

// GetTLSConfig gets the security config.
func (s *Server) GetTLSConfig() *grpcutil.TLSConfig {
	return &s.cfg.Security.TLSConfig
}

func (s *Server) startServer() (err error) {
	clusterID := keypath.ClusterID()
	// It may lose accuracy if use float64 to store uint64. So we store the cluster id in label.
	metaDataGauge.WithLabelValues(fmt.Sprintf("cluster%d", clusterID)).Set(0)
	// The independent TSO service still reuses PD version info since PD and TSO are just
	// different service modes provided by the same pd-server binary
	bs.ServerInfoGauge.WithLabelValues(versioninfo.PDReleaseVersion, versioninfo.PDGitHash).Set(float64(time.Now().Unix()))
	bs.ServerMaxProcsGauge.Set(float64(runtime.GOMAXPROCS(0)))

	// Initialize the TSO service.
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.Context())
	legacySvcRootPath := keypath.LegacyRootPath()
	tsoSvcRootPath := keypath.TSOSvcRootPath()
	s.keyspaceGroupManager = tso.NewKeyspaceGroupManager(
		s.serverLoopCtx, s.serviceID, s.GetClient(), s.GetHTTPClient(),
		s.cfg.AdvertiseListenAddr, legacySvcRootPath, tsoSvcRootPath, s.cfg)
	if err := s.keyspaceGroupManager.Initialize(); err != nil {
		return err
	}

	s.tsoProtoFactory = &tsoutil.TSOProtoFactory{}
	s.service = &Service{Server: s}

	if err := s.InitListener(s.GetTLSConfig(), s.cfg.ListenAddr); err != nil {
		return err
	}

	serverReadyChan := make(chan struct{})
	defer close(serverReadyChan)
	s.serverLoopWg.Add(1)
	go utils.StartGRPCAndHTTPServers(s, serverReadyChan, s.GetListener())
	<-serverReadyChan

	// Run callbacks
	log.Info("triggering the start callback functions")
	for _, cb := range s.GetStartCallbacks() {
		cb()
	}

	atomic.StoreInt64(&s.isRunning, 1)
	return nil
}

// CreateServer creates the Server
func CreateServer(ctx context.Context, cfg *Config) *Server {
	svr := &Server{
		BaseServer:        server.NewBaseServer(ctx),
		DiagnosticsServer: sysutil.NewDiagnosticsServer(cfg.Log.File.Filename),
		cfg:               cfg,
	}
	return svr
}

// CreateServerWrapper encapsulates the configuration/log/metrics initialization and create the server
func CreateServerWrapper(cmd *cobra.Command, args []string) {
	err := cmd.Flags().Parse(args)
	if err != nil {
		cmd.Println(err)
		return
	}
	cfg := NewConfig()
	flagSet := cmd.Flags()
	err = cfg.Parse(flagSet)
	defer logutil.LogPanic()

	if err != nil {
		cmd.Println(err)
		return
	}

	if printVersion, err := flagSet.GetBool("version"); err != nil {
		cmd.Println(err)
		return
	} else if printVersion {
		versioninfo.Print()
		utils.Exit(0)
	}

	// New zap logger
	err = logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	if err == nil {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	} else {
		log.Fatal("initialize logger error", errs.ZapError(err))
	}
	// Flushing any buffered log entries
	log.Sync()

	versioninfo.Log(serviceName)
	log.Info("TSO service config", zap.Reflect("config", cfg))

	grpcprometheus.EnableHandlingTimeHistogram()
	metricutil.Push(&cfg.Metric)

	ctx, cancel := context.WithCancel(context.Background())
	svr := CreateServer(ctx, cfg)

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

	if err := svr.Run(); err != nil {
		log.Fatal("run server failed", errs.ZapError(err))
	}

	<-ctx.Done()
	log.Info("got signal to exit", zap.String("signal", sig.String()))

	svr.Close()
	switch sig {
	case syscall.SIGTERM:
		utils.Exit(0)
	default:
		utils.Exit(1)
	}
}
