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

// NOTE: The code in this file is based on code from the
// etcd project, licensed under the Apache License v2.0
//
// https://github.com/etcd-io/etcd/blob/release-3.3/pkg/transport/listener.go
//

// Copyright 2015 The etcd Authors
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

package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/tikv/pd/client/errs"
)

// tlsInfo stores tls configuration to connect to etcd.
type tlsInfo struct {
	certFile           string
	keyFile            string
	trustedCAFile      string
	insecureSkipVerify bool

	// serverName ensures the cert matches the given host in case of discovery / virtual hosting
	serverName string

	// cipherSuites is a list of supported cipher suites.
	// If empty, Go auto-populates it by default.
	// Note that cipher suites are prioritized in the given order.
	cipherSuites []uint16

	selfCert bool

	// parseFunc exists to simplify testing. Typically, parseFunc
	// should be left nil. In that case, tls.X509KeyPair will be used.
	parseFunc func([]byte, []byte) (tls.Certificate, error)

	// allowedCNs is a list of CNs which must be provided by a client.
	allowedCNs []string
}

// clientConfig generates a tls.Config object for use by an HTTP client.
func (info tlsInfo) clientConfig() (*tls.Config, error) {
	var cfg *tls.Config
	var err error

	if !info.empty() {
		cfg, err = info.baseConfig()
		if err != nil {
			return nil, err
		}
	} else {
		cfg = &tls.Config{ServerName: info.serverName}
	}
	cfg.InsecureSkipVerify = info.insecureSkipVerify

	CAFiles := info.cafiles()
	if len(CAFiles) > 0 {
		cfg.RootCAs, err = NewCertPool(CAFiles)
		if err != nil {
			return nil, err
		}
	}

	if info.selfCert {
		cfg.InsecureSkipVerify = true
	}
	return cfg, nil
}

// empty returns if the TLSInfo is unset.
func (info tlsInfo) empty() bool {
	return info.certFile == "" && info.keyFile == ""
}

func (info tlsInfo) baseConfig() (*tls.Config, error) {
	if info.keyFile == "" || info.certFile == "" {
		return nil, fmt.Errorf("KeyFile and CertFile must both be present[key: %v, cert: %v]", info.keyFile, info.certFile)
	}

	_, err := NewCert(info.certFile, info.keyFile, info.parseFunc)
	if err != nil {
		return nil, err
	}

	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: info.serverName,
	}

	if len(info.cipherSuites) > 0 {
		cfg.CipherSuites = info.cipherSuites
	}

	if len(info.allowedCNs) > 0 {
		cfg.VerifyPeerCertificate = func(_ [][]byte, verifiedChains [][]*x509.Certificate) error {
			for _, chains := range verifiedChains {
				if len(chains) != 0 {
					for _, allowedCN := range info.allowedCNs {
						if allowedCN == chains[0].Subject.CommonName {
							return nil
						}
					}
				}
			}
			return errors.New("CommonName authentication failed")
		}
	}

	// this only reloads certs when there's a client request
	// TODO: support server-side refresh (e.g. inotify, SIGHUP), caching
	cfg.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		return NewCert(info.certFile, info.keyFile, info.parseFunc)
	}
	cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
		return NewCert(info.certFile, info.keyFile, info.parseFunc)
	}
	return cfg, nil
}

// cafiles returns a list of CA file paths.
func (info tlsInfo) cafiles() []string {
	cs := make([]string, 0)
	if info.trustedCAFile != "" {
		cs = append(cs, info.trustedCAFile)
	}
	return cs
}

// TLSConfig is the configuration for supporting tls.
type TLSConfig struct {
	// CAPath is the path of file that contains list of trusted SSL CAs. if set, following four settings shouldn't be empty
	CAPath string `toml:"cacert-path" json:"cacert-path"`
	// CertPath is the path of file that contains X509 certificate in PEM format.
	CertPath string `toml:"cert-path" json:"cert-path"`
	// KeyPath is the path of file that contains X509 key in PEM format.
	KeyPath string `toml:"key-path" json:"key-path"`
	// CertAllowedCNs is the list of CN which must be provided by a client
	CertAllowedCNs []string `toml:"cert-allowed-cn" json:"cert-allowed-cn"`

	SSLCABytes   []byte
	SSLCertBytes []byte
	SSLKEYBytes  []byte
}

// ToTLSConfig generates tls config.
func (s TLSConfig) ToTLSConfig() (*tls.Config, error) {
	if len(s.SSLCABytes) != 0 || len(s.SSLCertBytes) != 0 || len(s.SSLKEYBytes) != 0 {
		cert, err := tls.X509KeyPair(s.SSLCertBytes, s.SSLKEYBytes)
		if err != nil {
			return nil, errs.ErrCryptoX509KeyPair.GenWithStackByCause()
		}
		certificates := []tls.Certificate{cert}
		// Create a certificate pool from CA
		certPool := x509.NewCertPool()
		// Append the certificates from the CA
		if !certPool.AppendCertsFromPEM(s.SSLCABytes) {
			return nil, errs.ErrCryptoAppendCertsFromPEM.GenWithStackByCause()
		}
		return &tls.Config{
			Certificates: certificates,
			RootCAs:      certPool,
			NextProtos:   []string{"h2", "http/1.1"}, // specify `h2` to let Go use HTTP/2.
		}, nil
	}

	if len(s.CertPath) == 0 && len(s.KeyPath) == 0 {
		return nil, nil
	}

	tlsInfo := tlsInfo{
		certFile:      s.CertPath,
		keyFile:       s.KeyPath,
		trustedCAFile: s.CAPath,
		allowedCNs:    s.CertAllowedCNs,
	}

	tlsConfig, err := tlsInfo.clientConfig()
	if err != nil {
		return nil, errs.ErrEtcdTLSConfig.Wrap(err).GenWithStackByCause()
	}
	return tlsConfig, nil
}
