// Copyright 2022 TiKV Project Authors.
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
	"strings"
	"syscall"
	"time"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/tlsutil"
	"github.com/tools/pd-api-bench/cases"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	pdAddr = flag.String("pd", "127.0.0.1:2379", "pd address")

	httpCases = flag.String("http-cases", "", "http api cases")
	gRPCCases = flag.String("grpc-cases", "", "grpc cases")

	// concurrency
	concurrency = flag.Int("concurrency", 1, "client number")

	// tls
	caPath   = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath  = flag.String("key", "", "path of file that contains X509 key in PEM format")
)

var base int = int(time.Second) / int(time.Microsecond)

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

	addr := trimHTTPPrefix(*pdAddr)
	protocol := "http"
	if len(*caPath) != 0 {
		protocol = "https"
	}
	cases.PDAddress = fmt.Sprintf("%s://%s", protocol, addr)

	hcases := make([]cases.HTTPCase, 0)
	gcases := make([]cases.GRPCCase, 0)
	hcaseStr := strings.Split(*httpCases, ",")
	for _, str := range hcaseStr {
		if len(str) == 0 {
			continue
		}
		if cas, ok := cases.HTTPCaseMap[str]; ok {
			hcases = append(hcases, cas)
		} else {
			log.Println("no this case", str)
		}
	}
	gcaseStr := strings.Split(*gRPCCases, ",")
	for _, str := range gcaseStr {
		if len(str) == 0 {
			continue
		}
		if cas, ok := cases.GRPCCaseMap[str]; ok {
			gcases = append(gcases, cas)
		} else {
			log.Println("no this case", str)
		}
	}
	if *concurrency == 0 {
		log.Println("concurrency == 0, exit")
		return
	}
	pdClis := make([]pd.Client, 0)
	for i := 0; i < *concurrency; i++ {
		pdClis = append(pdClis, newPDClient())
	}
	httpClis := make([]*http.Client, 0)
	for i := 0; i < *concurrency; i++ {
		httpClis = append(httpClis, newHttpClient())
	}
	err := cases.InitCluster(ctx, pdClis[0], httpClis[0])
	if err != nil {
		log.Fatalf("InitCluster error %v", err)
	}

	for _, hcase := range hcases {
		handleHTTPCase(ctx, hcase, httpClis)
	}
	for _, gcase := range gcases {
		handleGRPCCase(ctx, gcase, pdClis)
	}

	<-ctx.Done()
	log.Println("Exit")
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func handleGRPCCase(ctx context.Context, gcase cases.GRPCCase, clients []pd.Client) {
	log.Printf("begin to run gRPC case %s", gcase.Name())
	qps := gcase.GetQPS()
	burst := gcase.GetBurst()
	tt := base / qps * burst * *concurrency
	for _, cli := range clients {
		go func(cli pd.Client) {
			var ticker = time.NewTicker(time.Duration(tt) * time.Microsecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					for i := 0; i < burst; i++ {
						err := gcase.Unary(ctx, cli)
						if err != nil {
							log.Println(err)
						}
					}
				case <-ctx.Done():
					log.Println("Got signal to exit handleGetRegion")
					return
				}
			}
		}(cli)
	}
}

func handleHTTPCase(ctx context.Context, hcase cases.HTTPCase, httpClis []*http.Client) {
	log.Printf("begin to run http case %s", hcase.Name())
	qps := hcase.GetQPS()
	burst := hcase.GetBurst()
	tt := base / qps * burst * *concurrency
	for _, hCli := range httpClis {
		go func(hCli *http.Client) {
			var ticker = time.NewTicker(time.Duration(tt) * time.Microsecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					for i := 0; i < burst; i++ {
						err := hcase.Do(ctx, hCli)
						if err != nil {
							log.Println(err)
						}
					}
				case <-ctx.Done():
					log.Println("Got signal to exit handleScanRegions")
					return
				}
			}
		}(hCli)
	}
}

func exit(code int) {
	os.Exit(code)
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

func trimHTTPPrefix(str string) string {
	str = strings.TrimPrefix(str, "http://")
	str = strings.TrimPrefix(str, "https://")
	return str
}

// newPDClient returns a pd client.
func newPDClient() pd.Client {
	const (
		keepaliveTime    = 10 * time.Second
		keepaliveTimeout = 3 * time.Second
	)

	addrs := []string{trimHTTPPrefix(*pdAddr)}
	pdCli, err := pd.NewClient(addrs, pd.SecurityOption{
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
