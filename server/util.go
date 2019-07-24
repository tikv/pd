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
	"regexp"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/etcdutil"
	"github.com/pingcap/pd/pkg/typeutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	requestTimeout  = etcdutil.DefaultRequestTimeout
	slowRequestTime = etcdutil.DefaultSlowRequestTime
	clientTimeout   = 3 * time.Second
)

// Version information.
var (
	PDReleaseVersion = "None"
	PDBuildTS        = "None"
	PDGitHash        = "None"
	PDGitBranch      = "None"
)

// dialClient used to dail http request.
var dialClient = &http.Client{
	Timeout: clientTimeout,
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

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

// CheckPDVersion checks if PD needs to be upgraded.
func CheckPDVersion(opt *scheduleOption) {
	pdVersion := MinSupportedVersion(Base)
	if PDReleaseVersion != "None" {
		pdVersion = *MustParseVersion(PDReleaseVersion)
	}
	clusterVersion := opt.loadClusterVersion()
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

// GetMembers return a slice of Members.
func GetMembers(etcdClient *clientv3.Client) ([]*pdpb.Member, error) {
	listResp, err := etcdutil.ListEtcdMembers(etcdClient)
	if err != nil {
		return nil, err
	}

	members := make([]*pdpb.Member, 0, len(listResp.Members))
	for _, m := range listResp.Members {
		info := &pdpb.Member{
			Name:       m.Name,
			MemberId:   m.ID,
			ClientUrls: m.ClientURLs,
			PeerUrls:   m.PeerURLs,
		}
		members = append(members, info)
	}

	return members, nil
}

func parseTimestamp(data []byte) (time.Time, error) {
	nano, err := typeutil.BytesToUint64(data)
	if err != nil {
		return zeroTime, err
	}

	return time.Unix(0, int64(nano)), nil
}

func subTimeByWallClock(after time.Time, before time.Time) time.Duration {
	return time.Duration(after.UnixNano() - before.UnixNano())
}

// InitHTTPClient initials a http client.
func InitHTTPClient(svr *Server) error {
	tlsConfig, err := svr.GetSecurityConfig().ToTLSConfig()
	if err != nil {
		return err
	}

	dialClient = &http.Client{
		Timeout: clientTimeout,
		Transport: &http.Transport{
			TLSClientConfig:   tlsConfig,
			DisableKeepAlives: true,
		},
	}
	return nil
}

const matchRule = "^[A-Za-z0-9]([-A-Za-z0-9_./]*[A-Za-z0-9])?$"

// ValidateLabelString checks the legality of the label string.
// The valid label consists of alphanumeric characters, '-', '_', '.' or '/',
// and must start and end with an alphanumeric character.
func ValidateLabelString(s string) error {
	isValid, _ := regexp.MatchString(matchRule, s)
	if !isValid {
		return errors.Errorf("invalid label: %s", s)
	}
	return nil
}

// ValidateLabels checks the legality of the labels.
func ValidateLabels(labels []*metapb.StoreLabel) error {
	for _, label := range labels {
		err := ValidateLabelString(label.Key)
		if err != nil {
			return err
		}
		err = ValidateLabelString(label.Value)
		if err != nil {
			return err
		}
	}
	return nil
}
