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
	"github.com/pingcap/kvproto/pkg/resource_manager"
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
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/memberutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var _ bs.Server = (*Server)(nil)

const serviceName = "Resource Manager"

// Server is the resource manager server, and it implements bs.Server.
type Server struct {
	*server.BaseServer
	diagnosticspb.DiagnosticsServer
	// Server state. 0 is not running, 1 is running.
	isRunning int64

	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	cfg *Config

	// for the primary election of resource manager
	participant *member.Participant

	service *Service

	// primaryCallbacks will be called after the server becomes leader.
	primaryCallbacks []func(context.Context) error

	// for service registry
	serviceID       *discovery.ServiceRegistryEntry
	serviceRegister *discovery.ServiceRegister
}

// Name returns the unique name for this server in the resource manager cluster.
func (s *Server) Name() string {
	return s.cfg.Name
}

// GetAddr returns the server address.
func (s *Server) GetAddr() string {
	return s.cfg.ListenAddr
}

// GetAdvertiseListenAddr returns the advertise address of the server.
func (s *Server) GetAdvertiseListenAddr() string {
	return s.cfg.AdvertiseListenAddr
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

// Run runs the Resource Manager server.
func (s *Server) Run() (err error) {
	if err = utils.InitClient(s); err != nil {
		return err
	}

	if s.serviceID, s.serviceRegister, err = utils.Register(s, constant.ResourceManagerServiceName); err != nil {
		return err
	}

	return s.startServer()
}

func (s *Server) startServerLoop() {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.Context())
	s.serverLoopWg.Add(1)
	go s.primaryElectionLoop()
}

func (s *Server) primaryElectionLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	for {
		select {
		case <-s.serverLoopCtx.Done():
			log.Info("server is closed, exit resource manager primary election loop")
			return
		default:
		}

		primary, checkAgain := s.participant.CheckLeader()
		if checkAgain {
			continue
		}
		if primary != nil {
			log.Info("start to watch the primary", zap.Stringer("resource-manager-primary", primary))
			// Watch will keep looping and never return unless the primary/leader has changed.
			primary.Watch(s.serverLoopCtx)
			log.Info("the resource manager primary has changed, try to re-campaign a primary")
		}

		s.campaignLeader()
	}
}

func (s *Server) campaignLeader() {
	log.Info("start to campaign the primary/leader", zap.String("campaign-resource-manager-primary-name", s.participant.Name()))
	if err := s.participant.CampaignLeader(s.Context(), s.cfg.LeaderLease); err != nil {
		if err.Error() == errs.ErrEtcdTxnConflict.Error() {
			log.Info("campaign resource manager primary meets error due to txn conflict, another server may campaign successfully",
				zap.String("campaign-resource-manager-primary-name", s.participant.Name()))
		} else {
			log.Error("campaign resource manager primary meets error due to etcd error",
				zap.String("campaign-resource-manager-primary-name", s.participant.Name()),
				errs.ZapError(err))
		}
		return
	}

	// Start keepalive the leadership and enable Resource Manager service.
	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	var resetLeaderOnce sync.Once
	defer resetLeaderOnce.Do(func() {
		cancel()
		s.participant.ResetLeader()
		member.ServiceMemberGauge.WithLabelValues(serviceName).Set(0)
	})

	// maintain the leadership, after this, Resource Manager could be ready to provide service.
	s.participant.KeepLeader(ctx)
	log.Info("campaign resource manager primary ok", zap.String("campaign-resource-manager-primary-name", s.participant.Name()))

	log.Info("triggering the primary callback functions")
	for _, cb := range s.primaryCallbacks {
		if err := cb(ctx); err != nil {
			log.Error("failed to trigger the primary callback function", errs.ZapError(err))
		}
	}

	s.participant.EnableLeader()
	member.ServiceMemberGauge.WithLabelValues(serviceName).Set(1)
	log.Info("resource manager primary is ready to serve", zap.String("resource-manager-primary-name", s.participant.Name()))

	leaderTicker := time.NewTicker(constant.LeaderTickInterval)
	defer leaderTicker.Stop()

	for {
		select {
		case <-leaderTicker.C:
			if !s.participant.IsLeader() {
				log.Info("no longer a primary/leader because lease has expired, the resource manager primary/leader will step down")
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Info("server is closed")
			return
		}
	}
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.isRunning, 1, 0) {
		// server is already closed
		return
	}

	log.Info("closing resource manager server ...")
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

	log.Info("resource manager server is closed")
}

// GetControllerConfig returns the controller config.
func (s *Server) GetControllerConfig() *ControllerConfig {
	return &s.cfg.Controller
}

// IsServing returns whether the server is the leader, if there is embedded etcd, or the primary otherwise.
func (s *Server) IsServing() bool {
	return !s.IsClosed() && s.participant.IsLeader()
}

// IsClosed checks if the server loop is closed
func (s *Server) IsClosed() bool {
	return s != nil && atomic.LoadInt64(&s.isRunning) == 0
}

// AddServiceReadyCallback adds callbacks when the server becomes the leader, if there is embedded etcd, or the primary otherwise.
func (s *Server) AddServiceReadyCallback(callbacks ...func(context.Context) error) {
	s.primaryCallbacks = append(s.primaryCallbacks, callbacks...)
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

// GetTLSConfig gets the security config.
func (s *Server) GetTLSConfig() *grpcutil.TLSConfig {
	return &s.cfg.Security.TLSConfig
}

// GetLeaderListenUrls gets service endpoints from the leader in election group.
func (s *Server) GetLeaderListenUrls() []string {
	return s.participant.GetLeaderListenUrls()
}

func (s *Server) startServer() (err error) {
	// The independent Resource Manager service still reuses PD version info since PD and Resource Manager are just
	// different service modes provided by the same pd-server binary
	bs.ServerInfoGauge.WithLabelValues(versioninfo.PDReleaseVersion, versioninfo.PDGitHash).Set(float64(time.Now().Unix()))
	bs.ServerMaxProcsGauge.Set(float64(runtime.GOMAXPROCS(0)))

	uniqueName := s.cfg.GetAdvertiseListenAddr()
	uniqueID := memberutil.GenerateUniqueID(uniqueName)
	log.Info("joining primary election", zap.String("participant-name", uniqueName), zap.Uint64("participant-id", uniqueID))
	s.participant = member.NewParticipant(s.GetClient(), constant.ResourceManagerServiceName)
	p := &resource_manager.Participant{
		Name:       uniqueName,
		Id:         uniqueID, // id is unique among all participants
		ListenUrls: []string{s.cfg.GetAdvertiseListenAddr()},
	}
	s.participant.InitInfo(p, keypath.ResourceManagerSvcRootPath(), constant.PrimaryKey, "primary election")

	s.service = &Service{
		ctx:     s.Context(),
		manager: NewManager[*Server](s),
	}

	if err := s.InitListener(s.GetTLSConfig(), s.cfg.GetListenAddr()); err != nil {
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
	// The start callback function will initialize storage, which will be used in service ready callback.
	// We should make sure the calling sequence is right.
	s.startServerLoop()

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
	log.Info("resource manager config", zap.Reflect("config", cfg))

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
