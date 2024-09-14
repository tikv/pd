// Copyright 2019 TiKV Project Authors.
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

package grpcutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/logutil"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	// ForwardMetadataKey is used to record the forwarded host of PD.
	ForwardMetadataKey = "pd-forwarded-host"
	// FollowerHandleMetadataKey is used to mark the permit of follower handle.
	FollowerHandleMetadataKey = "pd-allow-follower-handle"
)

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

// ToTLSInfo converts TLSConfig to transport.TLSInfo.
func (s TLSConfig) ToTLSInfo() (*transport.TLSInfo, error) {
	if len(s.CertPath) == 0 && len(s.KeyPath) == 0 {
		return nil, nil
	}

	return &transport.TLSInfo{
		CertFile:      s.CertPath,
		KeyFile:       s.KeyPath,
		TrustedCAFile: s.CAPath,
		AllowedCNs:    s.CertAllowedCNs,
	}, nil
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

	tlsInfo, err := s.ToTLSInfo()
	if tlsInfo == nil {
		return nil, nil
	}
	if err != nil {
		return nil, errs.ErrEtcdTLSConfig.Wrap(err).GenWithStackByCause()
	}

	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, errs.ErrEtcdTLSConfig.Wrap(err).GenWithStackByCause()
	}
	return tlsConfig, nil
}

// GetClientConn returns a gRPC client connection.
// creates a client connection to the given target. By default, it's
// a non-blocking dial (the function won't wait for connections to be
// established, and connecting happens in the background). To make it a blocking
// dial, use WithBlock() dial option.
//
// In the non-blocking case, the ctx does not act against the connection. It
// only controls the setup steps.
//
// In the blocking case, ctx can be used to cancel or expire the pending
// connection. Once this function returns, the cancellation and expiration of
// ctx will be noop. Users should call ClientConn.Close to terminate all the
// pending operations after this function returns.
func GetClientConn(ctx context.Context, addr string, tlsCfg *tls.Config, do ...grpc.DialOption) (*grpc.ClientConn, error) {
	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if tlsCfg != nil {
		creds := credentials.NewTLS(tlsCfg)
		opt = grpc.WithTransportCredentials(creds)
	}
	u, err := url.Parse(addr)
	if err != nil {
		return nil, errs.ErrURLParse.Wrap(err).GenWithStackByCause()
	}
	// Here we use a shorter MaxDelay to make the connection recover faster.
	// The default MaxDelay is 120s, which is too long for us.
	backoffOpts := grpc.WithConnectParams(grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  time.Second,
			Multiplier: 1.6,
			Jitter:     0.2,
			MaxDelay:   3 * time.Second,
		},
	})
	do = append(do, opt, backoffOpts)
	cc, err := grpc.DialContext(ctx, u.Host, do...)
	if err != nil {
		return nil, errs.ErrGRPCDial.Wrap(err).GenWithStackByCause()
	}
	return cc, nil
}

// BuildForwardContext creates a context with receiver metadata information.
// It is used in client side.
func BuildForwardContext(ctx context.Context, url string) context.Context {
	md := metadata.Pairs(ForwardMetadataKey, url)
	return metadata.NewOutgoingContext(ctx, md)
}

// ResetForwardContext is going to reset the forwarded host in metadata.
func ResetForwardContext(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		log.Error("failed to get forwarding metadata")
	}
	md.Set(ForwardMetadataKey, "")
	return metadata.NewOutgoingContext(ctx, md)
}

// GetForwardedHost returns the forwarded host in metadata.
func GetForwardedHost(ctx context.Context) string {
	s := metadata.ValueFromIncomingContext(ctx, ForwardMetadataKey)
	if len(s) > 0 {
		return s[0]
	}
	return ""
}

// IsFollowerHandleEnabled returns the follower host in metadata.
func IsFollowerHandleEnabled(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		log.Debug("failed to get gRPC incoming metadata when checking follower handle is enabled")
		return false
	}
	_, ok = md[FollowerHandleMetadataKey]
	return ok
}

func establish(ctx context.Context, addr string, tlsConfig *TLSConfig, do ...grpc.DialOption) (*grpc.ClientConn, error) {
	tlsCfg, err := tlsConfig.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	cc, err := GetClientConn(
		ctx,
		addr,
		tlsCfg,
		do...,
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return cc, nil
}

// CreateClientConn creates a client connection to the given target.
func CreateClientConn(ctx context.Context, addr string, tlsConfig *TLSConfig, do ...grpc.DialOption) *grpc.ClientConn {
	var (
		conn *grpc.ClientConn
		err  error
	)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		conn, err = establish(ctx, addr, tlsConfig, do...)
		if err != nil {
			log.Error("cannot establish connection", zap.String("addr", addr), errs.ZapError(err))
			continue
		}
		break
	}
	return conn
}

// CheckStream checks stream status, if stream is not created successfully in time, cancel context.
// TODO: If goroutine here timeout when tso stream created successfully, we need to handle it correctly.
func CheckStream(ctx context.Context, cancel context.CancelFunc, done chan struct{}) {
	defer logutil.LogPanic()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
		return
	case <-timer.C:
		cancel()
	case <-ctx.Done():
	}
	<-done
}

// NeedRebuildConnection checks if the error is a connection error.
func NeedRebuildConnection(err error) bool {
	return (err != nil) && (err == io.EOF ||
		strings.Contains(err.Error(), codes.Unavailable.String()) || // Unavailable indicates the service is currently unavailable. This is a most likely a transient condition.
		strings.Contains(err.Error(), codes.DeadlineExceeded.String()) || // DeadlineExceeded means operation expired before completion.
		strings.Contains(err.Error(), codes.Internal.String()) || // Internal errors.
		strings.Contains(err.Error(), codes.Unknown.String()) || // Unknown error.
		strings.Contains(err.Error(), codes.ResourceExhausted.String())) // ResourceExhausted is returned when either the client or the server has exhausted their resources.
	// Besides, we don't need to rebuild the connection if the code is Canceled, which means the client cancelled the request.
}
