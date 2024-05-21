// Copyright 2021 TiKV Project Authors.
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

package regiondump

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
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/tools/pd-dev/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
	"go.uber.org/zap"
)

const (
	etcdTimeout = 1200 * time.Second

	pdRootPath      = "/pd"
	maxKVRangeLimit = 10000
	minKVRangeLimit = 100
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

	if cfg.EndID != 0 && cfg.EndID < cfg.StartID {
		checkErr(errors.New("The end id should great or equal than start id"))
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

	err = loadRegions(client, cfg, f)
	checkErr(err)
	fmt.Println("successful!")
}

func regionPath(regionID uint64) string {
	return path.Join("raft", "r", fmt.Sprintf("%020d", regionID))
}

func loadRegions(client *clientv3.Client, cfg *Config, f *os.File) error {
	nextID := cfg.StartID
	endKey := regionPath(math.MaxUint64)
	if cfg.EndID != 0 {
		endKey = regionPath(cfg.EndID)
	}
	w := bufio.NewWriter(f)
	defer w.Flush()
	// Since the region key may be very long, using a larger rangeLimit will cause
	// the message packet to exceed the grpc message size limit (4MB). Here we use
	// a variable rangeLimit to work around.
	rangeLimit := maxKVRangeLimit
	for {
		startKey := regionPath(nextID)
		_, res, err := util.LoadRange(client, rootPath, startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= minKVRangeLimit {
				continue
			}
			return err
		}

		for _, s := range res {
			region := &metapb.Region{}
			if err := region.Unmarshal([]byte(s)); err != nil {
				return errors.WithStack(err)
			}
			nextID = region.GetId() + 1
			fmt.Fprintln(w, core.RegionToHexMeta(region).Region)
		}

		if len(res) < rangeLimit {
			return nil
		}
	}
}
