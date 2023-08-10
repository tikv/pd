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
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/tlsutil"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	pdAddr = flag.String("pd", "127.0.0.1:2379", "pd address")
	// min-resolved-ts
	minResolvedTSByGRPC = flag.Bool("min-resolved-ts-grpc", false, "min-resolved-ts by grpc")
	minResolvedTSByHTTP = flag.Bool("min-resolved-ts-http", false, "min-resolved-ts by http")
	// concurrency
	client = flag.Int("client", 1, "client number")
	// qps
	qps = flag.Int("qps", 1000, "qps")
	// tls
	caPath   = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath  = flag.String("key", "", "path of file that contains X509 key in PEM format")
)

var (
	base = int(time.Second) / int(time.Millisecond)
	wg   sync.WaitGroup
)

func main() {
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

	if *minResolvedTSByGRPC {
		go handleMinResolvedTSByGRPC(ctx)
	}
	if *minResolvedTSByHTTP {
		go handleMinResolvedTSByHTTP(ctx)
	}

	<-ctx.Done()
	wg.Wait()
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func handleMinResolvedTSByGRPC(ctx context.Context) {
	log.Println("handleMinResolvedTSByGRPC start...")
	tt := base / *qps
	wg.Add(*client)
	for i := 0; i < *client; i++ {
		pdCli := newPDClient()
		go func() {
			ticker := time.NewTicker(time.Millisecond * time.Duration(tt))
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					_, _, err := pdCli.GetMinResolvedTS(ctx, []uint64{1, 2, 3})
					if err != nil {
						log.Println(err)
						continue
					}
				case <-ctx.Done():
					log.Println("Got signal to exit handleMinResolvedTSByGRPC")
					wg.Done()
					return
				}
			}
		}()
	}
}

func handleMinResolvedTSByHTTP(ctx context.Context) {
	log.Println("handleMinResolvedTSByHTTP start...")
	// Judge whether with tls.
	protocol := "http"
	if len(*caPath) != 0 {
		protocol = "https"
	}
	url := fmt.Sprintf("%s://%s/pd/api/v1/min-resolved-ts", protocol, *pdAddr)

	tt := base / *qps
	httpsCli := newHttpClient()
	wg.Add(*client)
	for i := 0; i < *client; i++ {
		go func() {
			// Mock client-go's request frequency.
			ticker := time.NewTicker(time.Millisecond * time.Duration(tt))
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					storeID := 1
					req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%d", url, storeID), nil)
					res, err := httpsCli.Do(req)
					if err != nil {
						log.Println("request failed, error: ", err)
						continue
					}
					listResp := &minResolvedTS{}
					err = apiutil.ReadJSON(res.Body, listResp)
					if err != nil {
						log.Println("receive resp failed, error: ", err)
						continue
					}
					res.Body.Close()
				case <-ctx.Done():
					log.Println("Got signal to exit handleMinResolvedTSByHTTP")
					wg.Done()
					return
				}
			}
		}()
	}
}

type minResolvedTS struct {
	IsRealTime      bool              `json:"is_real_time,omitempty"`
	MinResolvedTS   uint64            `json:"min_resolved_ts"`
	PersistInterval typeutil.Duration `json:"persist_interval,omitempty"`
}

// newHttpClient returns an HTTP(s) client.
func newHttpClient() *http.Client {
	// defaultTimeout for non-context requests.
	const defaultTimeout = 30 * time.Second
	cli := &http.Client{Timeout: defaultTimeout}
	tlsConf := loadTLSConfig()
	if tlsConf != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConf
		cli.Transport = transport
	}
	return cli
}

// newPDClient returns a pd client.
func newPDClient() pd.Client {
	const (
		keepaliveTime    = 10 * time.Second
		keepaliveTimeout = 3 * time.Second
	)

	pdCli, err := pd.NewClient([]string{*pdAddr}, pd.SecurityOption{
		CAPath:   *caPath,
		CertPath: *certPath,
		KeyPath:  *keyPath,
	},
		pd.WithGRPCDialOptions(
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    keepaliveTime,
				Timeout: keepaliveTimeout,
			}),
		))
	if err != nil {
		log.Fatal("fail to create pd client", zap.Error(err))
	}
	return pdCli
}

func loadTLSConfig() *tls.Config {
	if len(*caPath) == 0 {
		return nil
	}
	caData, err := os.ReadFile(*caPath)
	if err != nil {
		log.Println("fail to read ca file", zap.Error(err))
	}
	certData, err := os.ReadFile(*certPath)
	if err != nil {
		log.Println("fail to read cert file", zap.Error(err))
	}
	keyData, err := os.ReadFile(*keyPath)
	if err != nil {
		log.Println("fail to read key file", zap.Error(err))
	}

	tlsConf, err := tlsutil.TLSConfig{
		SSLCABytes:   caData,
		SSLCertBytes: certData,
		SSLKEYBytes:  keyData,
	}.ToTLSConfig()
	if err != nil {
		log.Fatal("failed to load tlc config", zap.Error(err))
	}

	return tlsConf
}

func exit(code int) {
	os.Exit(code)
}
