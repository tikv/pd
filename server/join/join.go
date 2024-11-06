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

package join

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/server/config"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

const (
	// privateFileMode grants owner to read/write a file.
	privateFileMode = 0600
	// privateDirMode grants owner to make/remove files inside the directory.
	privateDirMode = 0700
)

// listMemberRetryTimes is the retry times of list member.
var listMemberRetryTimes = 20

// PrepareJoinCluster sends MemberAdd command to PD cluster,
// and returns the initial configuration of the PD cluster.
//
// TL;DR: The join functionality is safe. With data, join does nothing, w/o data
//
//	and it is not a member of cluster, join does MemberAdd, it returns an
//	error if PD tries to join itself, missing data or join a duplicated PD.
//
// Etcd automatically re-joins the cluster if there is a data directory. So
// first it checks if there is a data directory or not. If there is, it returns
// an empty string (etcd will get the correct configurations from the data
// directory.)
//
// If there is no data directory, there are following cases:
//
//   - A new PD joins an existing cluster.
//     What join does: MemberAdd, MemberList, then generate initial-cluster.
//
//   - A failed PD re-joins the previous cluster.
//     What join does: return an error. (etcd reports: raft log corrupted,
//     truncated, or lost?)
//
//   - A deleted PD joins to previous cluster.
//     What join does: MemberAdd, MemberList, then generate initial-cluster.
//     (it is not in the member list and there is no data, so
//     we can treat it as a new PD.)
//
// If there is a data directory, there are following special cases:
//
//   - A failed PD tries to join the previous cluster but it has been deleted
//     during its downtime.
//     What join does: return "" (etcd will connect to other peers and find
//     that the PD itself has been removed.)
//
//   - A deleted PD joins the previous cluster.
//     What join does: return "" (as etcd will read data directory and find
//     that the PD itself has been removed, so an empty string
//     is fine.)
func PrepareJoinCluster(cfg *config.Config) error {
	// - A PD tries to join itself.
	if cfg.Join == "" {
		return nil
	}

	if cfg.Join == cfg.AdvertiseClientUrls {
		return errors.New("join self is forbidden")
	}

	filePath := filepath.Join(cfg.DataDir, "join")
	// Read the persist join config
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		s, err := os.ReadFile(filePath)
		if err != nil {
			log.Fatal("read the join config meet error", errs.ZapError(errs.ErrIORead, err))
		}
		cfg.InitialCluster = strings.TrimSpace(string(s))
		cfg.InitialClusterState = embed.ClusterStateFlagExisting
		return nil
	}

	initialCluster := ""
	// Cases with data directory.
	if isDataExist(filepath.Join(cfg.DataDir, "member")) {
		cfg.InitialCluster = initialCluster
		cfg.InitialClusterState = embed.ClusterStateFlagExisting
		return nil
	}

	// Below are cases without data directory.
	tlsConfig, err := cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	lgc := zap.NewProductionConfig()
	lgc.Encoding = log.ZapEncodingName
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(cfg.Join, ","),
		DialTimeout: etcdutil.DefaultDialTimeout,
		TLS:         tlsConfig,
		LogConfig:   &lgc,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer client.Close()

	listResp, err := etcdutil.ListEtcdMembers(client.Ctx(), client)
	if err != nil {
		return err
	}

	existed := false
	for _, m := range listResp.Members {
		if len(m.Name) == 0 {
			log.Error("there is an abnormal joined member in the current member list",
				zap.Uint64("id", m.ID),
				zap.Strings("peer-urls", m.PeerURLs),
				zap.Strings("client-urls", m.ClientURLs))
			return errors.Errorf("there is a member %d that has not joined successfully", m.ID)
		}
		if m.Name == cfg.Name {
			existed = true
		}
	}

	// - A failed PD re-joins the previous cluster.
	if existed {
		return errors.New("missing data or join a duplicated pd")
	}

	var addResp *clientv3.MemberAddResponse

	failpoint.Inject("addMemberFailed", func() {
		listMemberRetryTimes = 2
		failpoint.Goto("LabelSkipAddMember")
	})
	// - A new PD joins an existing cluster.
	// - A deleted PD joins to previous cluster.
	{
		// First adds member through the API
		addResp, err = etcdutil.AddEtcdMember(client, []string{cfg.AdvertisePeerUrls})
		if err != nil {
			return err
		}
	}
	failpoint.Label("LabelSkipAddMember")

	var (
		pds      []string
		listSucc bool
	)

	for range listMemberRetryTimes {
		listResp, err = etcdutil.ListEtcdMembers(client.Ctx(), client)
		if err != nil {
			return err
		}

		pds = []string{}
		for _, memb := range listResp.Members {
			n := memb.Name
			if addResp != nil && memb.ID == addResp.Member.ID {
				n = cfg.Name
				listSucc = true
			}
			if len(n) == 0 {
				log.Error("there is an abnormal joined member in the current member list",
					zap.Uint64("id", memb.ID),
					zap.Strings("peer-urls", memb.PeerURLs),
					zap.Strings("client-urls", memb.ClientURLs))
				return errors.Errorf("there is a member %d that has not joined successfully", memb.ID)
			}
			for _, m := range memb.PeerURLs {
				pds = append(pds, fmt.Sprintf("%s=%s", n, m))
			}
		}

		if listSucc {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !listSucc {
		return errors.Errorf("join failed, adds the new member %s may failed", cfg.Name)
	}

	initialCluster = strings.Join(pds, ",")
	cfg.InitialCluster = initialCluster
	cfg.InitialClusterState = embed.ClusterStateFlagExisting
	err = os.MkdirAll(cfg.DataDir, privateDirMode)
	if err != nil && !os.IsExist(err) {
		return errors.WithStack(err)
	}

	err = os.WriteFile(filePath, []byte(cfg.InitialCluster), privateFileMode)
	return errors.WithStack(err)
}

func isDataExist(d string) bool {
	dir, err := os.Open(d)
	if err != nil {
		log.Info("failed to open directory, maybe start for the first time", errs.ZapError(err))
		return false
	}
	defer func() {
		if err := dir.Close(); err != nil {
			log.Error("failed to close file", errs.ZapError(err))
		}
	}()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		log.Error("failed to list directory", errs.ZapError(errs.ErrReadDirName, err))
		return false
	}
	return len(names) != 0
}
