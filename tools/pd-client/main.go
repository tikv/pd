package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/pingcap/pd/client"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

var (
	pdAddrs     = flag.String("pd", "127.0.0.1:2379", "pd address")
	concurrency = flag.Int("C", 1000, "concurrency")
	interval    = flag.Duration("interval", time.Second, "interval to output the statistics")
	caPath      = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs.")
	certPath    = flag.String("cert", "", "path of file that contains X509 certificate in PEM format..")
	keyPath     = flag.String("key", "", "path of file that contains X509 key in PEM format.")
	wg          sync.WaitGroup
)

func main() {
	flag.Parse()

	pdCli, err := pd.NewClient([]string{*pdAddrs}, pd.SecurityOption{
		CAPath:   *caPath,
		CertPath: *certPath,
		KeyPath:  *keyPath,
	})
	if err != nil {
		log.Fatal(fmt.Sprintf("%v", err))
	}

	ctx, _ := context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func(){
		<-sc
		os.Exit(0)
	}()



	pdCli.Watch(ctx,[]byte("/pd/"+strconv.FormatUint(pdCli.GetClusterID(ctx), 10)+"/config"))
}