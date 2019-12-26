// Copyright 2019 PingCAP, Inc.
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

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/pd/pkg/etcdutil"
	"github.com/pingcap/pd/pkg/typeutil"
	"github.com/pingcap/pd/server/config"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
)

var (
	pdAddr   = flag.String("pd", "http://127.0.0.1:2379", "pd address")
	filePath = flag.String("file", "backup.json", "the backup file path and name")
	caPath   = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs.")
	certPath = flag.String("cert", "", "path of file that contains X509 certificate in PEM format..")
	keyPath  = flag.String("key", "", "path of file that contains X509 key in PEM format.")
)

const (
	etcdTimeout     = 3 * time.Second
	pdRootPath      = "/pd"
	pdClusterIDPath = "/pd/cluster_id"
	pdConfigAPIPath = "/pd/api/v1/config"
)

type backupInfo struct {
	ClusterID         uint64         `json:"clusterID"`
	AllocIDMax        uint64         `json:"allocIDMax"`
	AllocTimestampMax uint64         `json:"allocTimestampMax"`
	Config            *config.Config `json:"config"`
}

func main() {
	flag.Parse()
	f, err := os.Create(*filePath)
	checkErr(err)
	defer f.Close()
	urls := strings.Split(*pdAddr, ",")

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

	backInfo := getBackupInfo(client)
	outputToFile(backInfo, f)
	fmt.Println("pd backup successful! dump file is:", *filePath)
}

func getBackupInfo(client *clientv3.Client) *backupInfo {
	backInfo := &backupInfo{}
	resp, err := etcdutil.EtcdKVGet(client, pdClusterIDPath)
	checkErr(err)
	clusterID, err := typeutil.BytesToUint64(resp.Kvs[0].Value)
	checkErr(err)
	backInfo.ClusterID = clusterID

	rootPath := path.Join(pdRootPath, strconv.FormatUint(clusterID, 10))
	allocIDPath := path.Join(rootPath, "alloc_id")
	resp, err = etcdutil.EtcdKVGet(client, allocIDPath)
	checkErr(err)
	allocIDMax, err := typeutil.BytesToUint64(resp.Kvs[0].Value)
	checkErr(err)
	backInfo.AllocIDMax = allocIDMax

	timestampPath := path.Join(rootPath, "timestamp")
	resp, err = etcdutil.EtcdKVGet(client, timestampPath)
	checkErr(err)
	allocTimestampMax, err := typeutil.BytesToUint64(resp.Kvs[0].Value)
	checkErr(err)
	backInfo.AllocTimestampMax = allocTimestampMax

	backInfo.Config = getConfig()
	return backInfo
}

func checkErr(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func outputToFile(backInfo *backupInfo, f *os.File) {
	w := bufio.NewWriter(f)
	defer w.Flush()
	backBytes, err := json.Marshal(backInfo)
	checkErr(err)
	var formatBuffer bytes.Buffer
	checkErr(json.Indent(&formatBuffer, []byte(backBytes), "", "    "))
	fmt.Fprintln(w, formatBuffer.String())
}

func getConfig() *config.Config {
	resp, err := http.Get(*pdAddr + pdConfigAPIPath)
	checkErr(err)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	checkErr(err)
	conf := &config.Config{}
	err = json.Unmarshal(body, conf)
	checkErr(err)
	return conf
}
