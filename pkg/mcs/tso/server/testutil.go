// Copyright 2023 TiKV Project Authors.
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
	"strings"

	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MustNewGrpcClient must create a new TSO grpc client.
func MustNewGrpcClient(re *require.Assertions, addr string) (*grpc.ClientConn, tsopb.TSOClient) {
	conn, err := grpc.Dial(strings.TrimPrefix(addr, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	re.NoError(err)
	return conn, tsopb.NewTSOClient(conn)
}

// GenerateConfig generates a new config with the given options.
func GenerateConfig(c *Config) (*Config, error) {
	arguments := []string{
		"--name=" + c.Name,
		"--listen-addr=" + c.ListenAddr,
		"--advertise-listen-addr=" + c.AdvertiseListenAddr,
		"--backend-endpoints=" + c.BackendEndpoints,
	}

	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flagSet.StringP("name", "", "", "human-readable name for this tso member")
	flagSet.BoolP("version", "V", false, "print version information and exit")
	flagSet.StringP("config", "", "", "config file")
	flagSet.StringP("backend-endpoints", "", "", "url for etcd client")
	flagSet.StringP("listen-addr", "", "", "listen address for tso service")
	flagSet.StringP("advertise-listen-addr", "", "", "advertise urls for listen address (default '${listen-addr}')")
	flagSet.StringP("cacert", "", "", "path of file that contains list of trusted TLS CAs")
	flagSet.StringP("cert", "", "", "path of file that contains X509 certificate in PEM format")
	flagSet.StringP("key", "", "", "path of file that contains X509 key in PEM format")
	err := flagSet.Parse(arguments)
	if err != nil {
		return nil, err
	}
	cfg := NewConfig()
	err = cfg.Parse(flagSet)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}
