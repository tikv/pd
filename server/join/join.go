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

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/server/config"
)

const (
	// privateDirMode grants owner to make/remove files inside the directory.
	privateDirMode = 0700
)

// listMemberRetryTimes is the retry times of list member.
var listMemberRetryTimes = 20

var (
	// restartListMemberRetryTimes is how many times a restarting member retries
	// listing cluster members (with exponential backoff) before skipping the
	// best-effort advertise-client-urls uniqueness check.
	restartListMemberRetryTimes = 3
	// restartListMemberBackoff is the initial backoff between those retries; it
	// doubles each time (2s, 4s, 8s), which together with the per-request
	// timeouts spans roughly 20s before giving up.
	restartListMemberBackoff = 2 * time.Second
)

// retryListEtcdMembers retries ListEtcdMembers with exponential backoff. It is
// used on the restart path so a transient failure does not immediately skip the
// client-URL uniqueness check; it returns the last error if all retries fail.
func retryListEtcdMembers(client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	var (
		listResp *clientv3.MemberListResponse
		err      error
	)
	backoff := restartListMemberBackoff
	for i := range restartListMemberRetryTimes {
		time.Sleep(backoff)
		backoff *= 2
		listResp, err = etcdutil.ListEtcdMembers(client.Ctx(), client)
		if err == nil {
			return listResp, nil
		}
		log.Warn("retry listing members on restart failed",
			zap.Int("retry", i+1), errs.ZapError(err))
	}
	return nil, err
}

// PrepareJoinCluster sends MemberAdd command to PD cluster,
// and returns the initial configuration of the PD cluster.
//
// TL;DR: The join functionality is safe. With data, join does nothing, w/o data
//
//	and it is not a member of cluster, join does MemberAdd, it returns an
//	error if PD tries to join itself, missing data or join a duplicated PD.
//	If previously the node joined but failed to start, it will attempt to
//	start as the same member again.
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
//   - A new PD joined an existing cluster but failed to start.
//     What join does: MemberList, then generate initial-cluster.
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

	initialCluster := ""
	dataExists := isDataExist(filepath.Join(cfg.DataDir, "member"))

	// A client to the target cluster is needed both to verify the advertised
	// client URL is unique and, for a fresh join, to run MemberAdd.
	tlsConfig, err := cfg.Security.ToClientTLSConfig()
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
		// A restart (data already exists) must not be blocked when the cluster
		// is unreachable; the member starts from its local data directory, so
		// the client-URL check is best-effort in that case.
		if dataExists {
			log.Warn("skip advertise-client-urls uniqueness check on restart: failed to create etcd client",
				errs.ZapError(err))
			cfg.InitialCluster = initialCluster
			cfg.InitialClusterState = embed.ClusterStateFlagExisting
			return nil
		}
		return errors.WithStack(err)
	}
	defer client.Close()

	listResp, err := etcdutil.ListEtcdMembers(client.Ctx(), client)
	if err != nil {
		if !dataExists {
			return err
		}
		// Restart: a transient failure should not silently skip the check.
		// Retry with exponential backoff, and only skip (best-effort) if the
		// cluster is still unreachable afterwards — the member must still be
		// able to start from its local data directory.
		listResp, err = retryListEtcdMembers(client)
		if err != nil {
			log.Warn("skip advertise-client-urls uniqueness check on restart: failed to list members after retries",
				errs.ZapError(err))
			cfg.InitialCluster = initialCluster
			cfg.InitialClusterState = embed.ClusterStateFlagExisting
			return nil
		}
	}

	// Reject a member — whether joining fresh or restarting with a modified
	// advertise-client-urls — whose advertised client URL is already owned by
	// another member. This is read-only (no quorum needed) and runs before the
	// local etcd (re)starts and republishes its attributes, so a duplicate never
	// enters the member list. See issue #10999.
	if err := checkClientURLConflict(
		strings.Split(cfg.AdvertiseClientUrls, ","),
		strings.Split(cfg.AdvertisePeerUrls, ","),
		listResp.Members,
	); err != nil {
		return err
	}

	// Cases with data directory: the local etcd reads its configuration from the
	// data directory, nothing more to prepare here. The registry claim is
	// intentionally skipped on this path — it writes to etcd (needs quorum), and
	// a restart must not depend on quorum.
	if dataExists {
		cfg.InitialCluster = initialCluster
		cfg.InitialClusterState = embed.ClusterStateFlagExisting
		return nil
	}

	// Below are cases without data directory.
	existed := false
	joinedFailedToStart := false
	advertisePeerURLs := strings.Split(cfg.AdvertisePeerUrls, ",")
	for _, m := range listResp.Members {
		if len(m.Name) == 0 {
			if slice.EqualWithoutOrder(m.PeerURLs, advertisePeerURLs) {
				log.Warn("the PD is already in the cluster but previously failed to start after join", zap.Any("member", m))
				joinedFailedToStart = true
			} else {
				log.Error("there is an abnormal joined member in the current member list",
					zap.Uint64("id", m.ID),
					zap.Strings("peer-urls", m.PeerURLs),
					zap.Strings("client-urls", m.ClientURLs))
				return errors.Errorf("there is a member %d that has not joined successfully", m.ID)
			}
		}
		if m.Name == cfg.Name {
			existed = true
		}
	}

	// - A failed PD re-joins the previous cluster.
	if existed {
		return errors.New("missing data or join a duplicated pd")
	}

	// Atomically claim the advertised client URLs before MemberAdd so two
	// concurrent joiners cannot both take the same client URL. Only the
	// fresh-join path reaches here, where quorum is required anyway.
	if err := claimClientURLs(
		client,
		listResp.Header.GetClusterId(),
		cfg.Name,
		strings.Split(cfg.AdvertiseClientUrls, ","),
		strings.Split(cfg.AdvertisePeerUrls, ","),
	); err != nil {
		return err
	}

	var addResp *clientv3.MemberAddResponse

	failpoint.Inject("addMemberFailed", func() {
		listMemberRetryTimes = 2
		failpoint.Goto("LabelSkipAddMember")
	})
	// - A new PD joins an existing cluster.
	// - A deleted PD joins to previous cluster.
	{
		// No need to add member if the PD is already in the cluster.
		if !joinedFailedToStart {
			// First adds member through the API
			addResp, err = etcdutil.AddEtcdMember(client, []string{cfg.AdvertisePeerUrls})
			if err != nil {
				return err
			}
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
				if joinedFailedToStart && slice.EqualWithoutOrder(memb.PeerURLs, advertisePeerURLs) {
					n = cfg.Name
					listSucc = true
				} else {
					log.Error("there is an abnormal joined member in the current member list",
						zap.Uint64("id", memb.ID),
						zap.Strings("peer-urls", memb.PeerURLs),
						zap.Strings("client-urls", memb.ClientURLs))
					return errors.Errorf("there is a member %d that has not joined successfully", memb.ID)
				}
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

	return nil
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
