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
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/tikv/pd/client/tlsutil"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pd "github.com/tikv/pd/client"
)

var (
	pdAddr = flag.String("pd", "127.0.0.1:2379", "pd address")
	// min-resolved-ts
	minResolvedTSByGRPC = flag.Int("min-resolved-ts-grpc", 0, "min-resolved-ts by grpc qps")
	minResolvedTSByHTTP = flag.Int("min-resolved-ts-http", 0, "min-resolved-ts by http qps")
	// concurrency
	concurrency = flag.Int("concurrency", 1, "client number")
	// tls
	caPath   = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath  = flag.String("key", "", "path of file that contains X509 key in PEM format")
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

	go handleMinResolvedTSByGRPC(ctx)
	go handleMinResolvedTSByHTTP(ctx)

	<-ctx.Done()
	log.Println("Exit")
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func handleMinResolvedTSByGRPC(ctx context.Context) {
	if *minResolvedTSByGRPC == 0 {
		log.Println("handleMinResolvedTSByGRPC qps = 0, exit")
		return
	}
	for i := 0; i < *concurrency; i++ {
		go func() {
			var ticker = time.NewTicker(time.Millisecond * 200)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					pdCli := newClient()
					_, _, err := pdCli.GetMinResolvedTimestamp(ctx, []uint64{1, 2, 3})
					if err != nil {
						log.Println(err)
						continue
					}
				case <-ctx.Done():
					log.Println("Got signal to exit handleScanRegions")
					return
				}
			}
		}()
	}
}

func handleMinResolvedTSByHTTP(ctx context.Context) {
	if *minResolvedTSByHTTP == 0 {
		log.Println("handleMinResolvedTSByHTTP qps = 0, exit")
		return
	}
	url := "https://" + *pdAddr + "/pd/api/v1/min-resolved-ts"
	for i := 0; i < *concurrency; i++ {
		go func() {
			// Mock client-go's request frequency.
			var ticker = time.NewTicker(time.Millisecond * 2000)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					storeID := 1
					req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%d", url, storeID), nil)
					httpsCli := httpClient()
					res, err := httpsCli.Do(req)
					if err != nil {
						log.Println("error: ", err)
						continue
					}
					listResp := &minResolvedTS{}
					apiutil.ReadJSON(res.Body, listResp)
					res.Body.Close()
					httpsCli.CloseIdleConnections()
				case <-ctx.Done():
					log.Println("Got signal to exit handleScanRegions")
					return
				}
			}
		}()
	}
}

type minResolvedTS struct {
	IsRealTime          bool              `json:"is_real_time,omitempty"`
	MinResolvedTS       uint64            `json:"min_resolved_ts"`
	PersistInterval     typeutil.Duration `json:"persist_interval,omitempty"`
	StoresMinResolvedTS map[uint64]uint64 `json:"stores_min_resolved_ts"`
}

// httpClient returns an HTTP(s) client.
func httpClient() *http.Client {
	tlsConf := loadTLSConfig()
	// defaultTimeout for non-context requests.
	const defaultTimeout = 30 * time.Second
	cli := &http.Client{Timeout: defaultTimeout}
	if tlsConf != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConf
		cli.Transport = transport
	}
	return cli
}

func loadTLSConfig() *tls.Config {
	caData, err := os.ReadFile(*caPath)
	if err != nil {
		println("fail to read ca file", err)
	}
	certData, err := os.ReadFile(*certPath)
	if err != nil {
		println("fail to read cert file", err)
	}
	keyData, err := os.ReadFile(*keyPath)
	if err != nil {
		println("fail to read key file", err)
	}

	tlsConf, err := tlsutil.TLSConfig{
		SSLCABytes:   caData,
		SSLCertBytes: certData,
		SSLKEYBytes:  keyData,
	}.ToTLSConfig()
	if err != nil {
		println("fail to load tls config", err)
	}
	return tlsConf
}

const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 3 * time.Second
)

// newClient returns a pd client.
func newClient() pd.Client {
	pdcli, err := pd.NewClient([]string{*pdAddr}, pd.SecurityOption{})
	if err != nil {
		println("fail to create pd client", err)
	}
	return pdcli
}

//func newClientWithCLS() pd.Client {
//	tlsConf := loadTLSConfig()
//	pdCli, err := pd.NewClient([]string{}, pd.SecurityOption{
//		SSLKEYBytes:  tlsConf.,
//		SSLCertBytes: certData,
//		SSLCABytes:   caData,
//	},
//		pd.WithGRPCDialOptions(
//			grpc.WithKeepaliveParams(keepalive.ClientParameters{
//				Time:    keepaliveTime,
//				Timeout: keepaliveTimeout,
//			}),
//		))
//	if err != nil {
//		println("fail to create pd client", err)
//	}
//	return pdCli
//}

func exit(code int) {
	os.Exit(code)
}
