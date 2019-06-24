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
	"encoding/binary"
	"fmt"
	"math/rand"
	"net/http"
	"regexp"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/etcdutil"
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

// PrintConfigCheckMsg prints the message about configuration checks.
func PrintConfigCheckMsg(cfg *Config) {
	if len(cfg.WarningMsgs) == 0 {
		fmt.Println("config check successful")
		return
	}

	for _, msg := range cfg.WarningMsgs {
		fmt.Println(msg)
	}
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

// A helper function to get value with key from etcd.
func getValue(c *clientv3.Client, key string, opts ...clientv3.OpOption) ([]byte, error) {
	resp, err := get(c, key, opts...)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

func get(c *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	resp, err := etcdutil.EtcdKVGet(c, key, opts...)
	if err != nil {
		return nil, err
	}

	if n := len(resp.Kvs); n == 0 {
		return nil, nil
	} else if n > 1 {
		return nil, errors.Errorf("invalid get value resp %v, must only one", resp.Kvs)
	}
	return resp, nil
}

// Return boolean to indicate whether the key exists or not.
func getProtoMsgWithModRev(c *clientv3.Client, key string, msg proto.Message, opts ...clientv3.OpOption) (bool, int64, error) {
	resp, err := get(c, key, opts...)
	if err != nil {
		return false, 0, err
	}
	if resp == nil {
		return false, 0, nil
	}
	value := resp.Kvs[0].Value
	if err = proto.Unmarshal(value, msg); err != nil {
		return false, 0, errors.WithStack(err)
	}
	return true, resp.Kvs[0].ModRevision, nil
}

func initOrGetClusterID(c *clientv3.Client, key string) (uint64, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), requestTimeout)
	defer cancel()

	// Generate a random cluster ID.
	ts := uint64(time.Now().Unix())
	clusterID := (ts << 32) + uint64(rand.Uint32())
	value := uint64ToBytes(clusterID)

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

	return bytesToUint64(response.Kvs[0].Value)
}

func bytesToUint64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, errors.Errorf("invalid data, must 8 bytes, but %d", len(b))
	}

	return binary.BigEndian.Uint64(b), nil
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// slowLogTxn wraps etcd transaction and log slow one.
type slowLogTxn struct {
	clientv3.Txn
	cancel context.CancelFunc
}

func newSlowLogTxn(client *clientv3.Client) clientv3.Txn {
	ctx, cancel := context.WithTimeout(client.Ctx(), requestTimeout)
	return &slowLogTxn{
		Txn:    client.Txn(ctx),
		cancel: cancel,
	}
}

func (t *slowLogTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	return &slowLogTxn{
		Txn:    t.Txn.If(cs...),
		cancel: t.cancel,
	}
}

func (t *slowLogTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	return &slowLogTxn{
		Txn:    t.Txn.Then(ops...),
		cancel: t.cancel,
	}
}

// Commit implements Txn Commit interface.
func (t *slowLogTxn) Commit() (*clientv3.TxnResponse, error) {
	start := time.Now()
	resp, err := t.Txn.Commit()
	t.cancel()

	cost := time.Since(start)
	if cost > slowRequestTime {
		log.Warn("txn runs too slow",
			zap.Error(err),
			zap.Reflect("response", resp),
			zap.Duration("cost", cost))
	}
	label := "success"
	if err != nil {
		label = "failed"
	}
	txnCounter.WithLabelValues(label).Inc()
	txnDuration.WithLabelValues(label).Observe(cost.Seconds())

	return resp, errors.WithStack(err)
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
	nano, err := bytesToUint64(data)
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
	tlsConfig, err := ToTLSConfig(svr.GetSecurityConfig())
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
