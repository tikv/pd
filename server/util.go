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
	"fmt"
	"math/rand"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/config"
	"github.com/urfave/negroni"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	requestTimeout = etcdutil.DefaultRequestTimeout
)

// LogPDInfo prints the PD version information.
func LogPDInfo() {
	log.Info("Welcome to Placement Driver (PD)")
	log.Info("PD", zap.String("release-version", versioninfo.PDReleaseVersion))
	log.Info("PD", zap.String("edition", versioninfo.PDEdition))
	log.Info("PD", zap.String("git-hash", versioninfo.PDGitHash))
	log.Info("PD", zap.String("git-branch", versioninfo.PDGitBranch))
	log.Info("PD", zap.String("utc-build-time", versioninfo.PDBuildTS))
}

// PrintPDInfo prints the PD version information without log info.
func PrintPDInfo() {
	fmt.Println("Release Version:", versioninfo.PDReleaseVersion)
	fmt.Println("Edition:", versioninfo.PDEdition)
	fmt.Println("Git Commit Hash:", versioninfo.PDGitHash)
	fmt.Println("Git Branch:", versioninfo.PDGitBranch)
	fmt.Println("UTC Build Time: ", versioninfo.PDBuildTS)
}

// PrintConfigCheckMsg prints the message about configuration checks.
func PrintConfigCheckMsg(cfg *config.Config) {
	if len(cfg.WarningMsgs) == 0 {
		fmt.Println("config check successful")
		return
	}

	for _, msg := range cfg.WarningMsgs {
		fmt.Println(msg)
	}
}

// CheckPDVersion checks if PD needs to be upgraded.
func CheckPDVersion(opt *config.PersistOptions) {
	pdVersion := versioninfo.MinSupportedVersion(versioninfo.Base)
	if versioninfo.PDReleaseVersion != "None" {
		pdVersion = versioninfo.MustParseVersion(versioninfo.PDReleaseVersion)
	}
	clusterVersion := *opt.GetClusterVersion()
	log.Info("load cluster version", zap.Stringer("cluster-version", clusterVersion))
	if pdVersion.LessThan(clusterVersion) {
		log.Warn(
			"PD version less than cluster version, please upgrade PD",
			zap.String("PD-version", pdVersion.String()),
			zap.String("cluster-version", clusterVersion.String()))
	}
}

func initOrGetClusterID(c *clientv3.Client, key string) (uint64, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), requestTimeout)
	defer cancel()

	// Generate a random cluster ID.
	ts := uint64(time.Now().Unix())
	clusterID := (ts << 32) + uint64(rand.Uint32())
	value := typeutil.Uint64ToBytes(clusterID)

	// Multiple PDs may try to init the cluster ID at the same time.
	// Only one PD can commit this transaction, then other PDs can get
	// the committed cluster ID.
	resp, err := c.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return 0, errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}

	// Txn commits ok, return the generated cluster ID.
	if resp.Succeeded {
		return clusterID, nil
	}

	// Otherwise, parse the committed cluster ID.
	if len(resp.Responses) == 0 {
		return 0, errs.ErrEtcdTxnConflict.FastGenByArgs()
	}

	response := resp.Responses[0].GetResponseRange()
	if response == nil || len(response.Kvs) != 1 {
		return 0, errs.ErrEtcdTxnConflict.FastGenByArgs()
	}

	return typeutil.BytesToUint64(response.Kvs[0].Value)
}

func checkBootstrapRequest(clusterID uint64, req *pdpb.BootstrapRequest) error {
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

/// REST API and GRPC services relative Utils.

// ServiceRegistry used to install the registered services, including gRPC and HTTP API.
type ServiceRegistry interface {
	InstallAllGRPCServices(srv *Server, g *grpc.Server)
	InstallAllRESTHandler(srv *Server, userDefineHandler map[string]http.Handler)
}

// NewServiceRegistry is a hook for msc code which implements the micro service.
var NewServiceRegistry = func() ServiceRegistry {
	return dummyServiceRegistry{}
}

type dummyServiceRegistry struct{}

func (d dummyServiceRegistry) InstallAllGRPCServices(srv *Server, g *grpc.Server) {
}

func (d dummyServiceRegistry) InstallAllRESTHandler(srv *Server, userDefineHandler map[string]http.Handler) {
}

// APIServiceGroup used to register the HTTP REST API.
type APIServiceGroup struct {
	Name       string
	Version    string
	IsCore     bool
	PathPrefix string
}

// Path returns the path of the service.
func (sg *APIServiceGroup) Path() string {
	if len(sg.PathPrefix) > 0 {
		return sg.PathPrefix
	}
	if sg.IsCore {
		return CorePath
	}
	if len(sg.Name) > 0 && len(sg.Version) > 0 {
		return path.Join(ExtensionsPath, sg.Name, sg.Version)
	}
	return ""
}

// RegisterUserDefinedHandlers register the user defined handlers.
func RegisterUserDefinedHandlers(registerMap map[string]http.Handler, group *APIServiceGroup, handler http.Handler) error {
	pathPrefix := group.Path()
	if _, ok := registerMap[pathPrefix]; ok {
		return errs.ErrServiceRegistered.FastGenByArgs(pathPrefix)
	}
	if len(pathPrefix) == 0 {
		return errs.ErrAPIInformationInvalid.FastGenByArgs(group.Name, group.Version)
	}
	registerMap[pathPrefix] = handler
	log.Info("register REST path", zap.String("path", pathPrefix))
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

		if err := RegisterUserDefinedHandlers(registerMap, &info, handler); err != nil {
			return nil, err
		}
	}

	// Combine the pd service to the router. the extension service will be added to the userHandlers.
	for pathPrefix, handler := range registerMap {
		if strings.Contains(pathPrefix, CorePath) || strings.Contains(pathPrefix, ExtensionsPath) {
			router.PathPrefix(pathPrefix).Handler(handler)
			if pathPrefix == CorePath {
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
