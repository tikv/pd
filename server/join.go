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
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"golang.org/x/net/context"
)

// the maximum amount of time a dial will wait for a connection to setup.
// 30s is long enough for most of the network conditions.
const defaultDialTimeout = 30 * time.Second

// TODO: support HTTPS
func (cfg *Config) genClientV3Config() clientv3.Config {
	endpoints := strings.Split(cfg.Join, ",")
	return clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: defaultDialTimeout,
	}
}

// prepareJoinCluster send MemberAdd command to pd cluster,
// returns pd initial cluster configuration.
func (cfg *Config) prepareJoinCluster() (string, error) {
	ctx, _ := context.WithTimeout(context.Background(), defaultDialTimeout)

	client, err := clientv3.New(cfg.genClientV3Config())
	if err != nil {
		return "", errors.Trace(err)
	}
	defer client.Close()

	scheme := "http"
	selfPeerURL := fmt.Sprintf("%s://%s:%d", scheme, cfg.Host, cfg.PeerPort)
	addResp, err := client.MemberAdd(ctx, []string{selfPeerURL})
	if err != nil {
		return "", errors.Trace(err)
	}

	log.Infof("Member %16x added to cluster %16x\n", addResp.Member.ID, addResp.Header.ClusterId)

	listResp, err := client.MemberList(ctx)
	if err != nil {
		return "", errors.Trace(err)
	}

	conf := []string{}
	for _, memb := range listResp.Members {
		for _, m := range memb.PeerURLs {
			n := memb.Name
			if memb.ID == addResp.Member.ID {
				n = cfg.Name
			}
			conf = append(conf, fmt.Sprintf("%s=%s", n, m))
		}
	}

	log.Infof("pd initial cluster: %s\n", strings.Join(conf, ","))
	return strings.Join(conf, ","), nil
}
