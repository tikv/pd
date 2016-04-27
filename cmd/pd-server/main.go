package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ngaut/log"
	"github.com/pingcap/pd/server"
)

var (
	addr          = flag.String("addr", "127.0.0.1:1234", "server listening address")
	advertiseAddr = flag.String("advertise-addr", "", "server advertise listening address [127.0.0.1:1234] for client communication")
	etcdAddrs     = flag.String("etcd", "127.0.0.1:2379", "Etcd endpoints, separated by comma")
	rootPath      = flag.String("root", "/pd", "pd root path in etcd")
	leaderLease   = flag.Int64("lease", 3, "leader lease time (second)")
	logLevel      = flag.String("L", "debug", "log level: info, debug, warn, error, fatal")
	pprofAddr     = flag.String("pprof", ":6060", "pprof HTTP listening address")
	clusterID     = flag.Uint64("cluster-id", 0, "cluster ID")
	maxPeerNumber = flag.Uint("max-peer-num", 3, "max peer number for the region")
)

func main() {
	flag.Parse()

	if *clusterID == 0 {
		log.Warn("cluster id is 0, don't use it in production")
	}

	log.SetLevelByString(*logLevel)

	go func() {
		http.ListenAndServe(*pprofAddr, nil)
	}()

	cfg := &server.Config{
		Addr:          *addr,
		AdvertiseAddr: *advertiseAddr,
		EtcdAddrs:     strings.Split(*etcdAddrs, ","),
		RootPath:      *rootPath,
		LeaderLease:   *leaderLease,
		ClusterID:     *clusterID,
		MaxPeerNumber: uint32(*maxPeerNumber),
	}

	svr, err := server.NewServer(cfg)
	if err != nil {
		log.Errorf("create pd server err %s\n", err)
		return
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("Got signal [%d] to exit.", sig)
		svr.Close()
		os.Exit(0)
	}()

	svr.Run()
}
