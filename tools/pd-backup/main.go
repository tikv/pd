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

package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tikv/pd/tools/pd-backup/pdbackup"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	pdAddr   = flag.String("pd", "http://127.0.0.1:2379", "pd address")
	filePath = flag.String("file", "backup.json", "backup file path and name")
	caPath   = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath  = flag.String("key", "", "path of file that contains X509 key in PEM format")
)

const (
	etcdTimeout = 3 * time.Second
)

func main() {
	flag.Parse()
	f, err := os.Create(*filePath)
	checkErr(err)
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Printf("error closing file: %s\n", err)
		}
	}()

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

	backInfo, err := pdbackup.GetBackupInfo(client, *pdAddr)
	checkErr(err)
	pdbackup.OutputToFile(backInfo, f)
	fmt.Println("pd backup successful! dump file is:", *filePath)
}

func checkErr(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
