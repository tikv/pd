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
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/coreos/etcd/pkg/transport"
	"golang.org/x/net/context"
)

// the maximum amount of time a dial will wait for a connection to setup.
// 30s is long enough for most of the network conditions.
const defaultDialTimeout = 30 * time.Second

// TODO: support HTTPS
func getTransport() (*http.Transport, error) {
	tls := transport.TLSInfo{}
	dialTimeout := defaultDialTimeout

	return transport.NewTransport(tls, dialTimeout)
}

func newClient(eps []string) (client.Client, error) {
	tr, err := getTransport()
	if err != nil {
		return nil, err
	}

	cfg := client.Config{
		Transport:               tr,
		Endpoints:               eps,
		HeaderTimeoutPerRequest: 0, // no timeout
	}

	return client.New(cfg)
}

// PrepareJoinCluster send MemberAdd command to pd cluster,
// returns pd initial cluster configuration.
func (cfg *Config) PrepareJoinCluster() string {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()

	eps := strings.Split(cfg.Join, ",")
	c, e := newClient(eps)
	if e != nil {
		fmt.Fprintln(os.Stderr, e.Error())
	}
	mAPI := client.NewMembersAPI(c)

	// TODO: support HTTPS
	scheme := "http"
	selfPeerURL := fmt.Sprintf("%s://%s:%d", scheme, cfg.Host, cfg.PeerPort)

	m, err := mAPI.Add(ctx, selfPeerURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	newID := m.ID
	name := cfg.Name
	fmt.Printf("Added member named %s with ID %s to cluster\n", name, newID)

	members, err := mAPI.List(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	conf := []string{}
	for _, memb := range members {
		for _, u := range memb.PeerURLs {
			n := memb.Name
			if memb.ID == newID {
				n = name
			}
			conf = append(conf, fmt.Sprintf("%s=%s", n, u))
		}
	}

	return strings.Join(conf, ",")
}
