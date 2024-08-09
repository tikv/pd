// Copyright 2019 TiKV Project Authors.
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

package health_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
	"go.etcd.io/etcd/client/pkg/v3/transport"
)

func TestHealth(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := pdTests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer tc.Destroy()
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	pdAddr := tc.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	client := tc.GetEtcdClient()
	members, err := cluster.GetMembers(client)
	re.NoError(err)
	healthMembers := cluster.CheckHealth(tc.GetHTTPClient(), members)
	healths := []api.Health{}
	for _, member := range members {
		h := api.Health{
			Name:       member.Name,
			MemberID:   member.MemberId,
			ClientUrls: member.ClientUrls,
			Health:     false,
		}
		if _, ok := healthMembers[member.GetMemberId()]; ok {
			h.Health = true
		}
		healths = append(healths, h)
	}

	// health command
	args := []string{"-u", pdAddr, "health"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	h := make([]api.Health, len(healths))
	re.NoError(json.Unmarshal(output, &h))
	re.Equal(healths, h)
}

func TestHealthTLS(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	certPath := filepath.Join("..", "cert")
	certScript := filepath.Join("..", "cert_opt.sh")
	// generate certs
	if err := os.Mkdir(certPath, 0755); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command(certScript, "generate", certPath).Run(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := exec.Command(certScript, "cleanup", certPath).Run(); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(certPath); err != nil {
			t.Fatal(err)
		}
	}()

	tlsInfo := transport.TLSInfo{
		KeyFile:       filepath.Join(certPath, "pd-server-key.pem"),
		CertFile:      filepath.Join(certPath, "pd-server.pem"),
		TrustedCAFile: filepath.Join(certPath, "ca.pem"),
	}
	tc, err := pdTests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Security.TLSConfig = grpcutil.TLSConfig{
			KeyPath:  tlsInfo.KeyFile,
			CertPath: tlsInfo.CertFile,
			CAPath:   tlsInfo.TrustedCAFile,
		}
		conf.AdvertiseClientUrls = strings.ReplaceAll(conf.AdvertiseClientUrls, "http", "https")
		conf.ClientUrls = strings.ReplaceAll(conf.ClientUrls, "http", "https")
		conf.AdvertisePeerUrls = strings.ReplaceAll(conf.AdvertisePeerUrls, "http", "https")
		conf.PeerUrls = strings.ReplaceAll(conf.PeerUrls, "http", "https")
		conf.InitialCluster = strings.ReplaceAll(conf.InitialCluster, "http", "https")
	})
	re.NoError(err)
	defer tc.Destroy()
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	cmd := ctl.GetRootCmd()

	client := tc.GetEtcdClient()
	members, err := cluster.GetMembers(client)
	re.NoError(err)
	healthMembers := cluster.CheckHealth(tc.GetHTTPClient(), members)
	healths := []api.Health{}
	for _, member := range members {
		h := api.Health{
			Name:       member.Name,
			MemberID:   member.MemberId,
			ClientUrls: member.ClientUrls,
			Health:     false,
		}
		if _, ok := healthMembers[member.GetMemberId()]; ok {
			h.Health = true
		}
		healths = append(healths, h)
	}

	pdAddr := tc.GetConfig().GetClientURL()
	pdAddr = strings.ReplaceAll(pdAddr, "http", "https")
	args := []string{"-u", pdAddr, "health",
		"--cacert=" + filepath.Join("..", "cert", "ca.pem"),
		"--cert=" + filepath.Join("..", "cert", "client.pem"),
		"--key=" + filepath.Join("..", "cert", "client-key.pem")}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	h := make([]api.Health, len(healths))
	re.NoError(json.Unmarshal(output, &h))
	re.Equal(healths, h)
}
