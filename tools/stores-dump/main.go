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

package main

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
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	clusterID = flag.Uint64("cluster-id", 0, "please make cluster ID match with TiKV")
	endpoints = flag.String("endpoints", "http://127.0.0.1:2379", "endpoints urls")
	filePath  = flag.String("file", "stores.dump", "dump file path and name")
	caPath    = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath  = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath   = flag.String("key", "", "path of file that contains X509 key in PEM format")
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

func main() {
	flag.Parse()

	rootPath = path.Join(pdRootPath, strconv.FormatUint(*clusterID, 10))
	f, err := os.Create(*filePath)
	checkErr(err)
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Printf("error closing file: %s\n", err)
		}
	}()

	urls := strings.Split(*endpoints, ",")

	tlsInfo := transport.TLSInfo{
		CertFile:      *certPath,
		KeyFile:       *keyPath,
		TrustedCAFile: *caPath,
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
		_, res, err := loadRange(client, key, endKey, minKVRangeLimit)
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

func loadRange(client *clientv3.Client, key, endKey string, limit int) ([]string, []string, error) {
	key = path.Join(rootPath, key)
	endKey = path.Join(rootPath, endKey)

	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(int64(limit))
	resp, err := etcdutil.EtcdKVGet(client, key, withRange, withLimit)
	if err != nil {
		return nil, nil, err
	}
	keys := make([]string, 0, len(resp.Kvs))
	values := make([]string, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		keys = append(keys, strings.TrimPrefix(strings.TrimPrefix(string(item.Key), rootPath), "/"))
		values = append(values, string(item.Value))
	}
	return keys, values, nil
}
