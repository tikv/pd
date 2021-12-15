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
	"bytes"
	"context"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/tikv/pd/pkg/apiutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	LoggerLabelLog       string = "log"
	LoggerLabelMonitored string = "Monitored"
)

var (
	componentSignatureKey   = "ti-component"
	componentAnonymousValue = "anonymous"
)

// SelfProtectionHandler a
type SelfProtectionHandler struct {
	// s *PDServer.Server
	// grpcServiceNames is used to find the service name of grpc method
	grpcServiceNames map[string]string
	// ServiceHandlers a
	ServiceHandlers map[string]*ServiceSelfProtectionHandler
}

func (h *SelfProtectionHandler) GetHTTPAPIServiceName(req *http.Request) (string, bool) {
	route := mux.CurrentRoute(req)
	if route != nil {
		if route.GetName() != "" {
			return route.GetName(), true
		}
	}
	return "", false
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

func (h *SelfProtectionHandler) SelfProtectionHandleHTTP(req *http.Request) bool {
	return true
}

func (h *SelfProtectionHandler) SelfProtectionHandleGRPC(fullMethod string, ctx context.Context) bool {
	return true
}

type ServiceSelfProtectionHandler struct {
	// apiRateLimiter *APIRateLimiter
	// auditLogger    *AuditLogger
}

func (h *ServiceSelfProtectionHandler) RateLimitAllow(componentName string) bool {
	return true
}

func (h *ServiceSelfProtectionHandler) EnableAudit() bool {
	return false
}

func GetLogInfoFromHTTP(req *http.Request, logInfo *LogInfo) {
	// Get IP
	logInfo.IP = apiutil.GetIPAddrFromHTTPRequest(req)
	// Get Param
	buf, err := io.ReadAll(req.Body)
	if err != nil {
		logInfo.Param = string(buf)
	}
	req.Body = io.NopCloser(bytes.NewBuffer(buf))
}

func GetLogInfoFromGRPC(ctx context.Context, logInfo *LogInfo) {
	// Get IP
	logInfo.IP = apiutil.GetIPAddrFromGRPCContext(ctx)
	// gRPC can't get Param in middware
}

func (h *ServiceSelfProtectionHandler) AuditLog(logInfo *LogInfo) {
}

type APIRateLimiter struct{}

type LogInfo struct {
	ServiceName    string
	Method         string
	Component      string
	IP             string
	TimeStamp      string
	Param          string
	RateLimitAllow bool
}

type AuditLogger struct{}

func (h *SelfProtectionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	if h.SelfProtectionHandleHTTP(r) {
		next(w, r)
	} else {
		http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
	}
}

func (h *SelfProtectionHandler) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return grpc.UnaryServerInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		if h.SelfProtectionHandleGRPC(info.FullMethod, ctx) {
			return nil, status.Errorf(codes.ResourceExhausted, "%s is rejected by grpc_ratelimit middleware, please retry later.", info.FullMethod)
		}
		return handler(ctx, req)
	})
}

func (h *SelfProtectionHandler) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if h.SelfProtectionHandleGRPC(info.FullMethod, stream.Context()) {
			return status.Errorf(codes.ResourceExhausted, "%s is rejected by grpc_ratelimit middleware, please retry later.", info.FullMethod)
		}
		return handler(srv, stream)
	}
}

// UserSignatureGRPCClientInterceptorBuilder add component user signature in gRPC
type UserSignatureGRPCClientInterceptorBuilder struct {
	component string
}

func (builder *UserSignatureGRPCClientInterceptorBuilder) SetComponentName(component string) {
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

func (rt *UserSignatureRoundTripper) RoundTrip(req *http.Request, component string) (resp *http.Response, err error) {
	req.Header.Set(componentSignatureKey, component)
	// Send the request, get the response and the error
	resp, err = rt.Proxied.RoundTrip(req)
	return
}
