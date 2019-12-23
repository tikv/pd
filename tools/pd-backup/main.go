package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/pingcap/pd/pkg/etcdutil"
	"github.com/pingcap/pd/pkg/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

var (
	pdAddr   = flag.String("pd", "http://127.0.0.1:2379", "pd address")
	filePath = flag.String("file", "backup.dump", "the backup file path and name")
	caPath   = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs.")
	certPath = flag.String("cert", "", "path of file that contains X509 certificate in PEM format..")
	keyPath  = flag.String("key", "", "path of file that contains X509 key in PEM format.")
)

const (
	etcdTimeout     = 3 * time.Second
	pdRootPath      = "/pd"
	pdClusterIDPath = "/pd/cluster_id"
	pdConfigApiPath = "/pd/api/v1/config"
)

type backupInfo struct {
	clusterID         uint64
	allocIDMax        uint64
	allocTimestampMax uint64
	config            string
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

	backOut := getBackupInfo(client)
	outputToFile(backOut, f)
	fmt.Println("pd backup successful! dump file is:", *filePath)
}

func getBackupInfo(client *clientv3.Client) *backupInfo {
	backOut := &backupInfo{}
	resp, err := etcdutil.EtcdKVGet(client, pdClusterIDPath)
	checkErr(err)
	clusterID, err := typeutil.BytesToUint64(resp.Kvs[0].Value)
	checkErr(err)
	backOut.clusterID = clusterID

	rootPath := path.Join(pdRootPath, strconv.FormatUint(clusterID, 10))
	allocIdPath := path.Join(rootPath, "alloc_id")
	resp, err = etcdutil.EtcdKVGet(client, allocIdPath)
	checkErr(err)
	allocIDMax, err := typeutil.BytesToUint64(resp.Kvs[0].Value)
	checkErr(err)
	backOut.allocIDMax = allocIDMax

	timestapPath := path.Join(rootPath, "timestamp")
	resp, err = etcdutil.EtcdKVGet(client, timestapPath)
	checkErr(err)
	allocTimestampMax, err := typeutil.BytesToUint64(resp.Kvs[0].Value)
	checkErr(err)
	backOut.allocTimestampMax = allocTimestampMax

	backOut.config = getConfig()
	return backOut
}

func checkErr(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func outputToFile(backOut *backupInfo, f *os.File) {
	w := bufio.NewWriter(f)
	defer w.Flush()
	clusterIDOut := "clusterID:" + strconv.FormatUint(backOut.clusterID, 10)
	allocIDMaxOut := "allocIDMax:" + strconv.FormatUint(backOut.allocIDMax, 10)
	allocTimestampMaxOut := "allocTimestampMax:" + strconv.FormatUint(backOut.allocTimestampMax, 10)
	fmt.Fprintln(w, clusterIDOut)
	fmt.Fprintln(w, allocIDMaxOut)
	fmt.Fprintln(w, allocTimestampMaxOut)
	fmt.Fprintln(w, "config:")
	fmt.Fprintln(w, backOut.config)
}

func getConfig() string {
	resp, err := http.Get(*pdAddr + pdConfigApiPath)
	checkErr(err)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	checkErr(err)
	return string(body)
}
