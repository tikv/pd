// Copyright 2016 TiKV Project Authors.
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
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/coreos/go-semver/semver"
	"github.com/gorilla/mux"
	gw "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/tmc/grpc-websocket-proxy/wsproxy"
	"github.com/urfave/negroni/v3"
	etcdservergw "go.etcd.io/etcd/api/v3/etcdserverpb/gw"
	"go.etcd.io/etcd/server/v3/embed"
	v3electiongw "go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb/gw"
	v3lockgw "go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb/gw"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/config"
)

// CheckAndGetPDVersion checks and returns the PD version.
func CheckAndGetPDVersion() *semver.Version {
	pdVersion := versioninfo.MinSupportedVersion(versioninfo.Base)
	if versioninfo.PDReleaseVersion != "None" {
		pdVersion = versioninfo.MustParseVersion(versioninfo.PDReleaseVersion)
	}
	return pdVersion
}

// CheckPDVersionWithClusterVersion checks if PD needs to be upgraded by comparing the PD version with the cluster version.
func CheckPDVersionWithClusterVersion(opt *config.PersistOptions) {
	pdVersion := CheckAndGetPDVersion()
	clusterVersion := *opt.GetClusterVersion()
	log.Info("load pd and cluster version",
		zap.Stringer("pd-version", pdVersion), zap.Stringer("cluster-version", clusterVersion))
	if pdVersion.LessThan(clusterVersion) {
		log.Warn(
			"PD version less than cluster version, please upgrade PD",
			zap.String("pd-version", pdVersion.String()),
			zap.String("cluster-version", clusterVersion.String()))
	}
}

func checkBootstrapRequest(req *pdpb.BootstrapRequest) error {
	clusterID := keypath.ClusterID()
	// TODO: do more check for request fields validation.

	storeMeta := req.GetStore()
	if storeMeta == nil {
		return errors.Errorf("missing store meta for bootstrap %d", clusterID)
	} else if storeMeta.GetId() == 0 {
		return errors.New("invalid zero store id")
	}

	regionMeta := req.GetRegion()
	if regionMeta == nil {
		return errors.Errorf("missing region meta for bootstrap %d", clusterID)
	} else if len(regionMeta.GetStartKey()) > 0 || len(regionMeta.GetEndKey()) > 0 {
		// first region start/end key must be empty
		return errors.Errorf("invalid first region key range, must all be empty for bootstrap %d", clusterID)
	} else if regionMeta.GetId() == 0 {
		return errors.New("invalid zero region id")
	}

	peers := regionMeta.GetPeers()
	if len(peers) != 1 {
		return errors.Errorf("invalid first region peer count %d, must be 1 for bootstrap %d", len(peers), clusterID)
	}

	peer := peers[0]
	if peer.GetStoreId() != storeMeta.GetId() {
		return errors.Errorf("invalid peer store id %d != %d for bootstrap %d", peer.GetStoreId(), storeMeta.GetId(), clusterID)
	}
	if peer.GetId() == 0 {
		return errors.New("invalid zero peer id")
	}

	return nil
}

func combineBuilderServerHTTPService(ctx context.Context, svr *Server, serviceBuilders ...HandlerBuilder) (map[string]http.Handler, error) {
	userHandlers := make(map[string]http.Handler)
	registerMap := make(map[string]http.Handler)

	apiService := negroni.New()
	recovery := negroni.NewRecovery()
	apiService.Use(recovery)
	router := mux.NewRouter()

	for _, build := range serviceBuilders {
		handler, info, err := build(ctx, svr)
		if err != nil {
			return nil, err
		}
		if !info.IsCore && len(info.PathPrefix) == 0 && (len(info.Name) == 0 || len(info.Version) == 0) {
			return nil, errs.ErrAPIInformationInvalid.FastGenByArgs(info.Name, info.Version)
		}

		if err := apiutil.RegisterUserDefinedHandlers(registerMap, &info, handler); err != nil {
			return nil, err
		}
	}

	// Combine the pd service to the router. the extension service will be added to the userHandlers.
	for pathPrefix, handler := range registerMap {
		if strings.Contains(pathPrefix, apiutil.CorePath) || strings.Contains(pathPrefix, apiutil.ExtensionsPath) {
			router.PathPrefix(pathPrefix).Handler(handler)
			if pathPrefix == apiutil.CorePath {
				// Deprecated
				router.Path("/pd/health").Handler(handler)
				// Deprecated
				router.Path("/pd/ping").Handler(handler)
			}
		} else {
			userHandlers[pathPrefix] = handler
		}
	}

	apiService.UseHandler(router)
	userHandlers[pdAPIPrefix] = apiService
	return userHandlers, nil
}

type etcdGateway struct {
	http.Handler
	etcd atomic.Pointer[embed.Etcd]
}

// newEtcdGatewayHandler creates a gateway for one embedded etcd lifetime.
// Its backend is published when etcd starts, independently of PD initialization.
func (s *Server) newEtcdGatewayHandler(cfg *embed.Config) (*etcdGateway, error) {
	tlsConfig, err := cfg.ClientTLSInfo.ServerConfig()
	if err != nil {
		return nil, err
	}
	// Match etcd's trust of its local endpoint, without copying the incoming
	// CN callback: InsecureSkipVerify leaves verifiedChains empty on clients.
	tlsConfig.InsecureSkipVerify = true
	tlsConfig.VerifyPeerCertificate = nil
	g := &etcdGateway{}
	gateway := sync.OnceValues(func() (http.Handler, error) {
		conn, err := grpc.NewClient(cfg.ListenClientUrls[0].Host,
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)))
		if err != nil {
			return nil, err
		}
		closeConn := func() {
			if err := conn.Close(); err != nil {
				log.Warn("failed to close etcd gateway connection", zap.Error(err))
			}
		}
		gwmux := gw.NewServeMux()
		for _, register := range []func(context.Context, *gw.ServeMux, *grpc.ClientConn) error{
			etcdservergw.RegisterKVHandler,
			etcdservergw.RegisterWatchHandler,
			etcdservergw.RegisterLeaseHandler,
			etcdservergw.RegisterClusterHandler,
			etcdservergw.RegisterMaintenanceHandler,
			etcdservergw.RegisterAuthHandler,
			v3lockgw.RegisterLockHandler,
			v3electiongw.RegisterElectionHandler,
		} {
			if err := register(s.ctx, gwmux, conn); err != nil {
				closeConn()
				return nil, err
			}
		}
		go func() {
			<-g.etcd.Load().Server.StopNotify()
			closeConn()
		}()
		return wsproxy.WebsocketProxy(gwmux,
			wsproxy.WithRequestMutator(func(_ *http.Request, outgoing *http.Request) *http.Request {
				outgoing.Method = http.MethodPost
				return outgoing
			}),
			wsproxy.WithMaxRespBodyBufferSize(math.MaxInt32),
			wsproxy.WithLogger(s.lg.Sugar())), nil
	})
	g.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		etcd := g.etcd.Load()
		if etcd == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		// Preserve etcd's rejection of CN-based gateway authentication when RBAC
		// is enabled; the internal connection must not impersonate the caller.
		if r.TLS != nil && etcd.Server.AuthStore().IsAuthEnabled() {
			for _, chain := range r.TLS.VerifiedChains {
				if len(chain) > 0 && chain[0].Subject.CommonName != "" {
					w.Header().Set("Content-Type", "text/plain; charset=utf-8")
					w.Header().Set("X-Content-Type-Options", "nosniff")
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte("CommonName of client sending a request against gateway will be ignored and not used as expected\n"))
					return
				}
			}
		}
		handler, err := gateway()
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		handler.ServeHTTP(w, r)
	})
	return g, nil
}
