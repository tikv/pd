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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	v             bool
	endpoints     string
	allocID       uint64
	clusterID     uint64
	caPath        string
	certPath      string
	keyPath       string
	fromOldMember bool
)

const (
	requestTimeout = 10 * time.Second
	etcdTimeout    = 3 * time.Second

	pdRootPath       = "/pd"
	pdClusterIDPath  = "/pd/cluster_id"
	allocIDSafeGuard = 100000000
)

func exitErr(err error) {
	fmt.Println(err.Error())
	os.Exit(1)
}

func main() {
	fs := flag.NewFlagSet("pd-recover", flag.ExitOnError)
	fs.BoolVar(&v, "V", false, "print version information")
	fs.BoolVar(&fromOldMember, "from-old-member", false, "recover from a member of an existing cluster")
	fs.StringVar(&endpoints, "endpoints", "http://127.0.0.1:2379", "endpoints urls")
	fs.Uint64Var(&allocID, "alloc-id", 0, "please make sure allocated ID is safe")
	fs.Uint64Var(&clusterID, "cluster-id", 0, "please make cluster ID match with tikv")
	fs.StringVar(&caPath, "cacert", "", "path of file that contains list of trusted SSL CAs")
	fs.StringVar(&certPath, "cert", "", "path of file that contains list of trusted SSL CAs")
	fs.StringVar(&keyPath, "key", "", "path of file that contains X509 key in PEM format")

	if len(os.Args[1:]) == 0 {
		fs.Usage()
		return
	}
	if err := fs.Parse(os.Args[1:]); err != nil {
		exitErr(err)
	}
	if v {
		versioninfo.Print()
		return
	}

	urls := strings.Split(endpoints, ",")

	tlsInfo := transport.TLSInfo{
		CertFile:      certPath,
		KeyFile:       keyPath,
		TrustedCAFile: caPath,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		fmt.Println("failed to connect: err")
		return
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   urls,
		DialTimeout: etcdTimeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		exitErr(err)
	}

	if fromOldMember {
		recoverFromOldMember(client)
		return
	}
	recoverFromNewPDCluster(client, clusterID, allocID)
}

func recoverFromNewPDCluster(client *clientv3.Client, clusterID, allocID uint64) {
	if clusterID == 0 {
		fmt.Println("please specify safe cluster-id")
		return
	}
	if allocID == 0 {
		fmt.Println("please specify safe alloc-id")
		return
	}
	rootPath := path.Join(pdRootPath, strconv.FormatUint(clusterID, 10))
	clusterRootPath := path.Join(rootPath, "raft")
	raftBootstrapTimeKey := path.Join(clusterRootPath, "status", "raft_bootstrap_time")

	ctx, cancel := context.WithTimeout(client.Ctx(), requestTimeout)
	defer cancel()

	var ops []clientv3.Op
	// recover cluster_id
	ops = append(ops, clientv3.OpPut(pdClusterIDPath, string(typeutil.Uint64ToBytes(clusterID))))
	// recover alloc_id
	allocIDPath := path.Join(rootPath, "alloc_id")
	ops = append(ops, clientv3.OpPut(allocIDPath, string(typeutil.Uint64ToBytes(allocID))))

	// recover bootstrap
	// recover meta of cluster
	clusterMeta := metapb.Cluster{Id: clusterID}
	clusterValue, err := clusterMeta.Marshal()
	if err != nil {
		exitErr(err)
	}
	ops = append(ops, clientv3.OpPut(clusterRootPath, string(clusterValue)))

	// set raft bootstrap time
	nano := time.Now().UnixNano()
	timeData := typeutil.Uint64ToBytes(uint64(nano))
	ops = append(ops, clientv3.OpPut(raftBootstrapTimeKey, string(timeData)))

	// the new pd cluster should not bootstrapped by tikv
	bootstrapCmp := clientv3.Compare(clientv3.CreateRevision(clusterRootPath), "=", 0)
	resp, err := client.Txn(ctx).If(bootstrapCmp).Then(ops...).Commit()
	if err != nil {
		exitErr(err)
	}
	if !resp.Succeeded {
		fmt.Println("failed to recover: the cluster is already bootstrapped")
		return
	}
	fmt.Println("recover success! please restart the PD cluster")
}

func recoverFromOldMember(client *clientv3.Client) {
	// cluster id
	resp, err := etcdutil.EtcdKVGet(client, pdClusterIDPath)
	if err != nil {
		exitErr(err)
	}
	clusterID, err := typeutil.BytesToUint64(resp.Kvs[0].Value)
	if err != nil {
		exitErr(err)
	}
	rootPath := path.Join(pdRootPath, strconv.FormatUint(clusterID, 10))
	clusterRootPath := path.Join(rootPath, "raft")

	// alloc id
	allocIDPath := path.Join(rootPath, "alloc_id")
	resp, err = etcdutil.EtcdKVGet(client, allocIDPath)
	if err != nil {
		exitErr(err)
	}
	var allocID uint64 = 0
	if resp.Count > 0 {
		allocID, err = typeutil.BytesToUint64(resp.Kvs[0].Value)
		if err != nil {
			exitErr(err)
		}
	}
	allocID += allocIDSafeGuard
	var ops []clientv3.Op
	// recover alloc_id
	ops = append(ops, clientv3.OpPut(allocIDPath, string(typeutil.Uint64ToBytes(allocID))))
	// delete stores
	storePath := path.Join(clusterRootPath, "s/")
	ops = append(ops, clientv3.OpDelete(storePath, clientv3.WithPrefix()))
	// the old pd cluster should bootstrapped by tikv
	bootstrapCmp := clientv3.Compare(clientv3.CreateRevision(clusterRootPath), "!=", 0)
	ctx, cancel := context.WithTimeout(client.Ctx(), requestTimeout)
	defer cancel()
	resp1, err := client.Txn(ctx).If(bootstrapCmp).Then(ops...).Commit()
	if err != nil {
		exitErr(err)
	}
	if !resp1.Succeeded {
		fmt.Println("failed to recover: the cluster is already bootstrapped")
		return
	}
	fmt.Println("recover success! please restart the PD cluster")
}
