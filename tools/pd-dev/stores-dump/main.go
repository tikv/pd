// Copyright 2020 TiKV Project Authors.
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

package storesdump

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/tools/pd-dev/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
	"go.uber.org/zap"
)

const (
	etcdTimeout     = 1200 * time.Second
	pdRootPath      = "/pd"
	minKVRangeLimit = 100
	clusterPath     = "raft"
)

var (
	rootPath = ""
)

func checkErr(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

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

	rootPath = path.Join(pdRootPath, strconv.FormatUint(cfg.ClusterID, 10))
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

	err = loadStores(client, f)
	checkErr(err)
	fmt.Println("successful!")
}

func loadStores(client *clientv3.Client, f *os.File) error {
	nextID := uint64(0)
	endKey := path.Join(clusterPath, "s", fmt.Sprintf("%020d", uint64(math.MaxUint64)))
	w := bufio.NewWriter(f)
	defer w.Flush()
	for {
		key := path.Join(clusterPath, "s", fmt.Sprintf("%020d", nextID))
		_, res, err := util.LoadRange(client, rootPath, key, endKey, minKVRangeLimit)
		if err != nil {
			return err
		}
		for _, str := range res {
			store := &metapb.Store{}
			if err := store.Unmarshal([]byte(str)); err != nil {
				return errors.WithStack(err)
			}

			nextID = store.GetId() + 1
			fmt.Fprintln(w, store)
		}
		if len(res) < minKVRangeLimit {
			return nil
		}
	}
}
