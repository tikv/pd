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
	"net/http"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/gorilla/mux"
	"github.com/urfave/negroni/v3"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	mcsconstant "github.com/tikv/pd/pkg/mcs/utils/constant"
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

func checkDefaultStoreLimitPersistenceFeature(component, name, gitHash, featureGitHash string) error {
	if featureGitHash == "" || featureGitHash != gitHash {
		return errors.Errorf(
			"cannot update default store limit while %s member %s does not support persisted default store limits",
			component, name)
	}
	return nil
}

// checkDefaultStoreLimitPersistenceSupport prevents a new persisted schedule
// field from being activated while an old PD or Scheduling Service member can
// still become leader/primary and drop the unknown field on its next persist.
func (s *Server) checkDefaultStoreLimitPersistenceSupport() error {
	members, err := s.ReloadMembers()
	if err != nil {
		return errors.Annotate(err, "failed to load PD members before updating default store limit")
	}
	for _, member := range members {
		gitHash, err := s.GetMember().GetMemberGitHash(member.GetMemberId())
		if err != nil {
			return errors.Annotatef(err, "failed to load git hash for PD member %s", member.GetName())
		}
		featureGitHash, err := s.GetMember().GetMemberFeature(
			member.GetMemberId(), versioninfo.DefaultStoreLimitPersistence)
		if err != nil {
			return errors.Errorf(
				"cannot update default store limit while PD member %s does not support persisted default store limits",
				member.GetName())
		}
		if err := checkDefaultStoreLimitPersistenceFeature(
			"PD", member.GetName(), gitHash, featureGitHash); err != nil {
			return err
		}
	}

	if !s.IsServiceIndependent(mcsconstant.SchedulingServiceName) {
		return nil
	}
	schedulingMembers, err := discovery.GetMSMembers(mcsconstant.SchedulingServiceName, s.GetClient())
	if err != nil {
		return errors.Annotate(err, "failed to load Scheduling Service members before updating default store limit")
	}
	if len(schedulingMembers) == 0 {
		return errors.New("cannot update default store limit without a registered Scheduling Service member")
	}
	for _, member := range schedulingMembers {
		if err := checkDefaultStoreLimitPersistenceFeature(
			"Scheduling Service", member.Name, member.GitHash,
			member.Features[versioninfo.DefaultStoreLimitPersistence]); err != nil {
			return err
		}
	}
	return nil
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
