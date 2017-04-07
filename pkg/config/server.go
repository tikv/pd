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

package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/embed"
	"github.com/juju/errors"
	"github.com/pingcap/pd/pkg/typeutil"
)

const (
	defaultLeaderLease             = int64(3)
	defaultNextRetryDelay          = time.Second
	defaultAutoCompactionRetention = 1

	defaultName                = "pd"
	defaultClientUrls          = "http://127.0.0.1:2379"
	defaultPeerUrls            = "http://127.0.0.1:2380"
	defualtInitialClusterState = embed.ClusterStateFlagNew

	// etcd use 100ms for heartbeat and 1s for election timeout.
	// We can enlarge both a little to reduce the network aggression.
	// now embed etcd use TickMs for heartbeat, we will update
	// after embed etcd decouples tick and heartbeat.
	defaultTickMs = uint64(500)
	// embed etcd has a check that `5 * tick > election`
	defaultElectionMs = uint64(3000)
)

// ServerConfig contains all basic server config.
type ServerConfig struct {
	Name    string `toml:"name" json:"name"`
	DataDir string `toml:"data-dir" json:"data-dir"`

	ClientUrls          string `toml:"client-urls" json:"client-urls"`
	PeerUrls            string `toml:"peer-urls" json:"peer-urls"`
	AdvertiseClientUrls string `toml:"advertise-client-urls" json:"advertise-client-urls"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls" json:"advertise-peer-urls"`

	InitialCluster      string `toml:"initial-cluster" json:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state" json:"initial-cluster-state"`

	// Join to an existing pd cluster, a string of endpoints.
	Join string `toml:"join" json:"join"`

	// LeaderLease time, if leader doesn't update its TTL
	// in etcd after lease time, etcd will expire the leader key
	// and other servers can campaign the leader again.
	// Etcd onlys support seoncds TTL, so here is second too.
	LeaderLease int64 `toml:"lease" json:"lease"`

	// Log level.
	LogLevel string `toml:"log-level" json:"log-level"`
	// Log file.
	LogFile string `toml:"log-file" json:"log-file"`

	// TsoSaveInterval is the interval to save timestamp.
	TsoSaveInterval typeutil.Duration `toml:"tso-save-interval" json:"tso-save-interval"`

	// QuotaBackendBytes Raise alarms when backend size exceeds the given quota. 0 means use the default quota.
	// the default size is 2GB, the maximum is 8GB.
	QuotaBackendBytes typeutil.ByteSize `toml:"quota-backend-bytes" json:"quota-backend-bytes"`
	// AutoCompactionRetention for mvcc key value store in hour. 0 means disable auto compaction.
	// the default retention is 1 hour
	AutoCompactionRetention int `toml:"auto-compaction-retention" json:"auto-compaction-retention"`

	// Only used for test
	NextRetryDelay             time.Duration
	DisableStrictReconfigCheck bool

	tickMs     uint64
	electionMs uint64
}

func (c *ServerConfig) validate() error {
	if c.Join != "" && c.InitialCluster != "" {
		return errors.New("-initial-cluster and -join can not be provided at the same time")
	}
	return nil
}

func (c *ServerConfig) adjust() {
	adjustString(&c.Name, defaultName)
	adjustString(&c.DataDir, fmt.Sprintf("default.%s", c.Name))

	adjustString(&c.ClientUrls, defaultClientUrls)
	adjustString(&c.AdvertiseClientUrls, c.ClientUrls)
	adjustString(&c.PeerUrls, defaultPeerUrls)
	adjustString(&c.AdvertisePeerUrls, c.PeerUrls)

	if len(c.InitialCluster) == 0 {
		// The advertise peer urls may be http://127.0.0.1:2380,http://127.0.0.1:2381
		// so the initial cluster is pd=http://127.0.0.1:2380,pd=http://127.0.0.1:2381
		items := strings.Split(c.AdvertisePeerUrls, ",")

		sep := ""
		for _, item := range items {
			c.InitialCluster += fmt.Sprintf("%s%s=%s", sep, c.Name, item)
			sep = ","
		}
	}

	adjustString(&c.InitialClusterState, defualtInitialClusterState)

	adjustInt64(&c.LeaderLease, defaultLeaderLease)

	adjustDuration(&c.TsoSaveInterval, time.Duration(defaultLeaderLease)*time.Second)

	if c.NextRetryDelay == 0 {
		c.NextRetryDelay = defaultNextRetryDelay
	}
	if c.AutoCompactionRetention == 0 {
		c.AutoCompactionRetention = defaultAutoCompactionRetention
	}

	adjustUint64(&c.tickMs, defaultTickMs)
	adjustUint64(&c.electionMs, defaultElectionMs)

}
