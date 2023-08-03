// Copyright 2023 TiKV Project Authors.
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
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	pdAddr = flag.String("pd", "127.0.0.1:2379", "pd address")
	// min-resolved-ts
	minResolvedTSQPS = flag.Int("min-resolved-ts", 0, "min-resolved-ts qps")
	// store max id
	maxStoreID = flag.Int("max-store", 100, "store max id")
	// concurrency
	concurrency = flag.Int("concurrency", 1, "client number")
	// burst
	burst = flag.Int("burst", 1, "burst request")
)

func main() {
	log.SetFlags(0)
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	go handleMinResolvedTS(ctx)
	<-ctx.Done()
	log.Println("Exit")
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func loadTLSContent(caPath, certPath, keyPath string) (caData, certData, keyData []byte) {
	var err error
	caData, err = os.ReadFile(caPath)
	if err != nil {
		log.Fatal("fail to read ca file", zap.Error(err))
	}
	certData, err = os.ReadFile(certPath)
	if err != nil {
		log.Fatal("fail to read cert file", zap.Error(err))
	}
	keyData, err = os.ReadFile(keyPath)
	if err != nil {
		log.Fatal("fail to read key file", zap.Error(err))
	}
	return
}
func handleMinResolvedTS(ctx context.Context) {
	if *minResolvedTSQPS == 0 {
		log.Println("handleMinResolvedTS qps = 0, exit")
		return
	}
	for i := 0; i < 10; i++ {
		go func() {
			var ticker = time.NewTicker(time.Millisecond * 200)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					pdCli := newClient()
					resolvedTS, storesMap, err := pdCli.GetMinResolvedTimestamp(ctx, []uint64{1, 2, 3})
					if err != nil {
						log.Println(err)
						continue
					}
					println("resolvedTS", resolvedTS)
					println("storesMap", storesMap)
				case <-ctx.Done():
					log.Println("Got signal to exit handleScanRegions")
					return
				}
			}
		}()
	}
}

func exit(code int) {
	os.Exit(code)
}

const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 3 * time.Second
)

func newClient() pd.Client {
	pdcli, err := pd.NewClient([]string{*pdAddr}, pd.SecurityOption{})
	if err != nil {
		println("fail to create pd client", err)
	}
	return pdcli
}

func newClientWithCLS() pd.Client {
	println("newClient")
	caData, certData, keyData := loadTLSContent(
		"./cert/ca.pem",
		"./cert/client.csr",
		"./cert/client-key.pem")
	pdCli, err := pd.NewClient([]string{}, pd.SecurityOption{
		SSLKEYBytes:  keyData,
		SSLCertBytes: certData,
		SSLCABytes:   caData,
	},
		pd.WithGRPCDialOptions(
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    keepaliveTime,
				Timeout: keepaliveTimeout,
			}),
		))
	if err != nil {
		println("fail to create pd client", err)
	}
	return pdCli
}
