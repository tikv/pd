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

package grpcutil

import (
	"context"
	"crypto/tls"
	"net/url"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/pkg/retry"
)

const (
	dialTimeout = 3 * time.Second
	// ForwardMetadataKey is used to record the forwarded host of PD.
	ForwardMetadataKey = "pd-forwarded-host"
	// FollowerHandleMetadataKey is used to mark the permit of follower handle.
	FollowerHandleMetadataKey = "pd-allow-follower-handle"
)

// UnaryBackofferInterceptor is a gRPC interceptor that adds a backoffer to the call.
func UnaryBackofferInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		bo := retry.FromContext(ctx)
		if bo == nil {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		// Copy a new backoffer
		newBo := *bo
		var lastErr error
		err := newBo.Exec(ctx, func() error {
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err != nil {
				lastErr = err
				return err
			}
			return nil
		})

		if err != nil {
			return lastErr
		}
		return nil
	}
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

	// Add backoffer interceptor
	retryOpt := grpc.WithUnaryInterceptor(UnaryBackofferInterceptor())

	// Add retry related connection parameters
	backoffOpts := grpc.WithConnectParams(grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  time.Second,
			Multiplier: 1.6,
			Jitter:     0.2,
			MaxDelay:   3 * time.Second,
		},
	})

	do = append(do, opt, retryOpt, backoffOpts)
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

// GetForwardedHost returns the forwarded host in metadata.
// Only used for test.
func GetForwardedHost(ctx context.Context, f func(context.Context) (metadata.MD, bool)) string {
	v, _ := getValueFromMetadata(ctx, ForwardMetadataKey, f)
	return v
}

// BuildFollowerHandleContext creates a context with follower handle metadata information.
// It is used in client side.
func BuildFollowerHandleContext(ctx context.Context) context.Context {
	md := metadata.Pairs(FollowerHandleMetadataKey, "")
	return metadata.NewOutgoingContext(ctx, md)
}

// IsFollowerHandleEnabled returns the forwarded host in metadata.
// Only used for test.
func IsFollowerHandleEnabled(ctx context.Context, f func(context.Context) (metadata.MD, bool)) bool {
	_, ok := getValueFromMetadata(ctx, FollowerHandleMetadataKey, f)
	return ok
}

func getValueFromMetadata(ctx context.Context, key string, f func(context.Context) (metadata.MD, bool)) (string, bool) {
	md, ok := f(ctx)
	if !ok {
		return "", false
	}
	vs, ok := md[key]
	if !ok {
		return "", false
	}
	return vs[0], true
}

// GetOrCreateGRPCConn returns the corresponding grpc client connection of the given addr.
// Returns the old one if's already existed in the clientConns; otherwise creates a new one and returns it.
func GetOrCreateGRPCConn(ctx context.Context, clientConns *sync.Map, url string, tlsCfg *tls.Config, opt ...grpc.DialOption) (*grpc.ClientConn, error) {
	conn, ok := clientConns.Load(url)
	if ok {
		// TODO: check the connection state.
		return conn.(*grpc.ClientConn), nil
	}
	dCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	cc, err := GetClientConn(dCtx, url, tlsCfg, opt...)
	failpoint.Inject("unreachableNetwork2", func(val failpoint.Value) {
		if val, ok := val.(string); ok && val == url {
			cc = nil
			err = errors.Errorf("unreachable network")
		}
	})
	if err != nil {
		return nil, err
	}
	conn, loaded := clientConns.LoadOrStore(url, cc)
	if !loaded {
		// Successfully stored the connection.
		return cc, nil
	}
	cc.Close()
	cc = conn.(*grpc.ClientConn)
	log.Debug("use existing connection", zap.String("target", cc.Target()), zap.String("state", cc.GetState().String()))
	return cc, nil
}
