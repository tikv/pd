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

package backup

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/tools/pd-dev/pd-backup/pdbackup"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
	"go.uber.org/zap"
)

const (
	etcdTimeout = 3 * time.Second
)

func Run() {
	cfg := newConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Fatal("parse cmd flags error", zap.Error(err))
	}

	f, err := os.Create(cfg.FilePath)
	checkErr(err)
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Printf("error closing file: %s\n", err)
		}
	}()

	urls := strings.Split(cfg.PDAddrs, ",")

	tlsInfo := transport.TLSInfo{
		CertFile:      cfg.CertPath,
		KeyFile:       cfg.KeyPath,
		TrustedCAFile: cfg.CaPath,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	checkErr(err)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   urls,
		DialTimeout: etcdTimeout,
		TLS:         tlsConfig,
	})
	checkErr(err)

	backInfo, err := pdbackup.GetBackupInfo(client, cfg.PDAddrs)
	checkErr(err)
	pdbackup.OutputToFile(backInfo, f)
	fmt.Println("pd backup successful! dump file is:", cfg.FilePath)
}

func checkErr(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
