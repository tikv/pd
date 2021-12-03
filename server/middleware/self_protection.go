// Copyright 2016 TiKV Project Authors.
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

package middleware

import (
	"context"
	"net/http"
	"sync"

	PDServer "github.com/tikv/pd/server"

	"google.golang.org/grpc/metadata"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/grpc"

	"golang.org/x/time/rate"
)

var (
	componentSignatureKey   = "ti-component"
	componentAnonymousValue = "anonymous"
)

// SelfProtectionHandler a
type SelfProtectionHandler struct {
	s *PDServer.Server
	// apiServiceNames is used to find the service name of api
	apiServiceNames map[string]string
	// grpcServiceNames is used to find the service name of grpc method
	grpcServiceNames map[string]string
	// ServiceHandlers a
	ServiceHandlers map[string]ServiceSelfProtectionHandler
}

func (h *SelfProtectionHandler) GetHttpApiServiceName(url string) (string, bool) {
	serviceName, ok := h.apiServiceNames[url]
	return serviceName, ok
}

func (h *SelfProtectionHandler) GetGRPCServiceName(method string) (string, bool) {
	serviceName, ok := h.grpcServiceNames[method]
	return serviceName, ok
}

func (h *SelfProtectionHandler) GetComponentNameOnHTTP(r *http.Request) string {
	componentName := r.Header.Get(componentSignatureKey)
	if componentName == "" {
		componentName = componentAnonymousValue
	}
	return componentName
}

func (h *SelfProtectionHandler) GetComponentNameOnGRPC(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		componentNameList, ok := md[componentSignatureKey]
		if ok && len(componentNameList) > 1 {
			return componentNameList[0]
		}
	}
	return componentAnonymousValue
}

func (h *SelfProtectionHandler) SelfProtectionHandle(componentName string, serviceName string) bool {
	serviceHandler, ok := h.ServiceHandlers[serviceName]
	if !ok {
		return true
	}
	limitAllow := serviceHandler.apiRateLimiter.Allow(componentName)

	return limitAllow
}

type ServiceSelfProtectionHandler struct {
	apiRateLimiter ApiRateLimiter
	auditLog       AuditLog
}

type ApiRateLimiter struct {
	mu sync.RWMutex

	enableQpsLimit bool

	totalQpsRateLimiter *rate.Limiter

	enableComponentQpsLimit bool
	userQpsRateLimiter      map[string]*rate.Limiter
}

func (rl *ApiRateLimiter) QpsAllow(componentName string) bool {
	if !rl.enableQpsLimit {
		return true
	}
	isComponentQpsLimit, isTotalQpsLimit := true, true
	if rl.enableComponentQpsLimit {
		componentRateLimiter, ok := rl.userQpsRateLimiter[componentName]
		if !ok {
			componentRateLimiter = rl.userQpsRateLimiter[componentAnonymousValue]
		}
		isComponentQpsLimit = componentRateLimiter.Allow()
	}
	isTotalQpsLimit = rl.totalQpsRateLimiter.Allow()
	return isComponentQpsLimit && isTotalQpsLimit
}

func (rl *ApiRateLimiter) Allow(componentName string) bool {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.QpsAllow(componentName)
}

func (h *SelfProtectionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	serviceName, foundName := h.GetHttpApiServiceName(r.URL.Path)
	if !foundName {
		next(w, r)
		return
	}
	componentSignature := h.GetComponentNameOnHTTP(r)
	if h.SelfProtectionHandle(componentSignature, serviceName) {
		next(w, r)
	} else {
		http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
	}
}

func (h *SelfProtectionHandler) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return grpc.UnaryServerInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		serviceName, foundName := h.GetGRPCServiceName(info.FullMethod)
		if !foundName {
			return handler(ctx, req)
		}
		componentSignature := h.GetComponentNameOnGRPC(ctx)
		if h.SelfProtectionHandle(componentSignature, serviceName) {
			return nil, status.Errorf(codes.ResourceExhausted, "%s is rejected by grpc_ratelimit middleware, please retry later.", info.FullMethod)
		}
		return handler(ctx, req)
	})
}

func (h *SelfProtectionHandler) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		serviceName, foundName := h.GetGRPCServiceName(info.FullMethod)
		if !foundName {
			return handler(srv, stream)
		}
		componentSignature := h.GetComponentNameOnGRPC(stream.Context())
		if h.SelfProtectionHandle(componentSignature, serviceName) {
			return status.Errorf(codes.ResourceExhausted, "%s is rejected by grpc_ratelimit middleware, please retry later.", info.FullMethod)
		}
		return handler(srv, stream)
	}
}

type AuditLog struct {
}

// UserSignatureGRPCClientInterceptorBuilder add component user signature in gRPC
type UserSignatureGRPCClientInterceptorBuilder struct {
	component string
}

func (builder *UserSignatureGRPCClientInterceptorBuilder) setComponentName(component string) {
	builder.component = component
}

func (builder *UserSignatureGRPCClientInterceptorBuilder) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		headerData := metadata.Pairs(componentSignatureKey, builder.component)
		ctxH := metadata.NewOutgoingContext(ctx, headerData)
		err := invoker(ctxH, method, req, reply, cc, opts...)
		return err
	}
}

func (builder *UserSignatureGRPCClientInterceptorBuilder) StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		headerData := metadata.Pairs(componentSignatureKey, builder.component)
		ctxH := metadata.NewOutgoingContext(ctx, headerData)
		return streamer(ctxH, desc, cc, method, opts...)
	}
}

func (builder *UserSignatureGRPCClientInterceptorBuilder) UserSignatureDialOptions() []grpc.DialOption {
	streamInterceptors := []grpc.StreamClientInterceptor{builder.StreamClientInterceptor()}
	unaryInterceptors := []grpc.UnaryClientInterceptor{builder.UnaryClientInterceptor()}
	opts := []grpc.DialOption{grpc.WithChainStreamInterceptor(streamInterceptors...), grpc.WithChainUnaryInterceptor(unaryInterceptors...)}
	return opts
}

// UserSignatureRoundTripper add component user signature in http
type UserSignatureRoundTripper struct {
	Proxied http.RoundTripper
}

func (rt *UserSignatureRoundTripper) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	req.Header.Set(componentSignatureKey, "pd-ctl")
	// Send the request, get the response and the error
	resp, err = rt.Proxied.RoundTrip(req)
	return
}
