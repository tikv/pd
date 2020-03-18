// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"path"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/etcdutil"
	"github.com/pingcap/pd/v4/pkg/typeutil"
	"github.com/pingcap/pd/v4/server/cluster"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	clientTimeout  = 3 * time.Second
	requestTimeout = etcdutil.DefaultRequestTimeout
)

// Version information.
var (
	PDReleaseVersion = "None"
	PDBuildTS        = "None"
	PDGitHash        = "None"
	PDGitBranch      = "None"
)

// LogPDInfo prints the PD version information.
func LogPDInfo() {
	log.Info("Welcome to Placement Driver (PD)")
	log.Info("PD", zap.String("release-version", PDReleaseVersion))
	log.Info("PD", zap.String("git-hash", PDGitHash))
	log.Info("PD", zap.String("git-branch", PDGitBranch))
	log.Info("PD", zap.String("utc-build-time", PDBuildTS))
}

// PrintPDInfo prints the PD version information without log info.
func PrintPDInfo() {
	fmt.Println("Release Version:", PDReleaseVersion)
	fmt.Println("Git Commit Hash:", PDGitHash)
	fmt.Println("Git Branch:", PDGitBranch)
	fmt.Println("UTC Build Time: ", PDBuildTS)
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
func CheckPDVersion(opt *config.ScheduleOption) {
	pdVersion := *cluster.MinSupportedVersion(cluster.Base)
	if PDReleaseVersion != "None" {
		pdVersion = *cluster.MustParseVersion(PDReleaseVersion)
	}
	clusterVersion := *opt.LoadClusterVersion()
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
		return 0, errors.WithStack(err)
	}

	// Txn commits ok, return the generated cluster ID.
	if resp.Succeeded {
		return clusterID, nil
	}

	// Otherwise, parse the committed cluster ID.
	if len(resp.Responses) == 0 {
		return 0, errors.Errorf("txn returns empty response: %v", resp)
	}

	response := resp.Responses[0].GetResponseRange()
	if response == nil || len(response.Kvs) != 1 {
		return 0, errors.Errorf("txn returns invalid range response: %v", resp)
	}

	return typeutil.BytesToUint64(response.Kvs[0].Value)
}

// InitHTTPClient initials a http client.
func InitHTTPClient(svr *Server) error {
	tlsConfig, err := svr.GetSecurityConfig().ToTLSConfig()
	if err != nil {
		return err
	}

	cluster.DialClient = &http.Client{
		Timeout: clientTimeout,
		Transport: &http.Transport{
			TLSClientConfig:   tlsConfig,
			DisableKeepAlives: true,
		},
	}
	return nil
}

func makeStoreKey(clusterRootPath string, storeID uint64) string {
	return path.Join(clusterRootPath, "s", fmt.Sprintf("%020d", storeID))
}

func makeRegionKey(clusterRootPath string, regionID uint64) string {
	return path.Join(clusterRootPath, "r", fmt.Sprintf("%020d", regionID))
}

func makeRaftClusterStatusPrefix(clusterRootPath string) string {
	return path.Join(clusterRootPath, "status")
}

func makeBootstrapTimeKey(clusterRootPath string) string {
	return path.Join(makeRaftClusterStatusPrefix(clusterRootPath), "raft_bootstrap_time")
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

func isTiFlashStore(store *metapb.Store) bool {
	for _, l := range store.GetLabels() {
		if l.GetKey() == "engine" && l.GetValue() == "tiflash" {
			return true
		}
	}
	return false
}
