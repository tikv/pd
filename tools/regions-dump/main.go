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
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/server/core"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
)

var (
	clusterID = flag.Uint64("cluster-id", 0, "please make cluster ID match with TiKV")
	endpoints = flag.String("endpoints", "http://127.0.0.1:2379", "endpoints urls")
	startID   = flag.Uint64("start-id", 0, "ID of the start region")
	endID     = flag.Uint64("end-id", 0, "ID of the last region")
	filePath  = flag.String("file", "regions.dump", "dump file path and name")
	caPath    = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath  = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath   = flag.String("key", "", "path of file that contains X509 key in PEM format")
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

func main() {
	flag.Parse()
	if *endID != 0 && *endID < *startID {
		checkErr(errors.New("The end id should great or equal than start id"))
	}
	rootPath = path.Join(pdRootPath, strconv.FormatUint(*clusterID, 10))
	f, err := os.Create(*filePath)
	checkErr(err)
	defer f.Close()

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

	err = loadRegions(client, f)
	checkErr(err)
	fmt.Println("successful!")
}

func regionPath(regionID uint64) string {
	return path.Join("raft", "r", fmt.Sprintf("%020d", regionID))
}

func loadRegions(client *clientv3.Client, f *os.File) error {
	nextID := *startID
	endKey := regionPath(math.MaxUint64)
	if *endID != 0 {
		endKey = regionPath(*endID)
	}
	w := bufio.NewWriter(f)
	defer w.Flush()
	// Since the region key may be very long, using a larger rangeLimit will cause
	// the message packet to exceed the grpc message size limit (4MB). Here we use
	// a variable rangeLimit to work around.
	rangeLimit := maxKVRangeLimit
	for {
		startKey := regionPath(nextID)
		_, res, err := loadRange(client, startKey, endKey, rangeLimit)
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
