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
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/apiutil"
	PDServer "github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
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
	s *PDServer.Server
	// grpcServiceNames is used to find the service name of grpc method
	grpcServiceNames map[string]string
	// ServiceHandlers a
	ServiceHandlers map[string]*ServiceSelfProtectionHandler
}

func NewSelfProtectionHandler(server *PDServer.Server) *SelfProtectionHandler {
	handler := &SelfProtectionHandler{s: server,
		grpcServiceNames: config.GRPCMethodServiceNames,
		ServiceHandlers:  make(map[string]*ServiceSelfProtectionHandler),
	}
	handler.UpdateServiceHandlers()
	return handler
}

func (h *SelfProtectionHandler) UpdateServiceHandlers() {
	if h.s == nil {
		return
	}
	enableUseDefault := h.s.GetConfig().SelfProtectionConfig.EnableUseDefault
	h.ServiceHandlers = make(map[string]*ServiceSelfProtectionHandler)
	// if enableUseDefault is 2, only use config defined by users
	if enableUseDefault == 2 {
		for i := range h.s.GetConfig().SelfProtectionConfig.ServiceSelfprotectionConfig {
			serviceName := h.s.GetConfig().SelfProtectionConfig.ServiceSelfprotectionConfig[i].ServiceName
			serviceSelfProtectionHandler := NewServiceSelfProtectionHandler(&h.s.GetConfig().SelfProtectionConfig.ServiceSelfprotectionConfig[i])
			h.ServiceHandlers[serviceName] = serviceSelfProtectionHandler
		}
		// if enableUseDefault is 1, config defined by users has higher priority than dafault
	} else if enableUseDefault == 1 {
		mergeSelfProtectionConfig(h.ServiceHandlers, h.s.GetConfig().SelfProtectionConfig.ServiceSelfprotectionConfig, config.DefaultServiceSelfProtectionConfig)
		// if enableUseDefault is 0, dafault config has higher priority than config defined by users
	} else {
		mergeSelfProtectionConfig(h.ServiceHandlers, config.DefaultServiceSelfProtectionConfig, h.s.GetConfig().SelfProtectionConfig.ServiceSelfprotectionConfig)
	}
}

func mergeSelfProtectionConfig(handlers map[string]*ServiceSelfProtectionHandler, highPriorityConfigs []config.ServiceSelfprotectionConfig, lowPriorityConfigs []config.ServiceSelfprotectionConfig) {
	for i := range highPriorityConfigs {
		serviceName := highPriorityConfigs[i].ServiceName
		serviceSelfProtectionHandler := NewServiceSelfProtectionHandler(&highPriorityConfigs[i])
		handlers[serviceName] = serviceSelfProtectionHandler
	}
	for i := range lowPriorityConfigs {
		serviceName := lowPriorityConfigs[i].ServiceName
		if _, find := handlers[serviceName]; find {
			continue
		}
		serviceSelfProtectionHandler := NewServiceSelfProtectionHandler(&lowPriorityConfigs[i])
		handlers[serviceName] = serviceSelfProtectionHandler
	}
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
	serviceName, foundName := h.GetHTTPAPIServiceName(req)
	if !foundName {
		return true
	}
	componentSignature := h.GetComponentNameOnHTTP(req)
	serviceHandler, ok := h.ServiceHandlers[serviceName]
	if !ok {
		return true
	}
	limitAllow := serviceHandler.RateLimitAllow(componentSignature)
	if serviceHandler.EnableAudit() {
		logInfo := &LogInfo{
			ServiceName:    serviceName,
			Method:         fmt.Sprintf("HTTP-%s:%s", req.Method, req.URL.Path),
			Component:      componentSignature,
			TimeStamp:      time.Now().Local().String(),
			RateLimitAllow: limitAllow,
		}
		GetLogInfoFromHTTP(req, logInfo)
		serviceHandler.AuditLog(logInfo)
	}
	return limitAllow
}

func (h *SelfProtectionHandler) SelfProtectionHandleGRPC(fullMethod string, ctx context.Context) bool {
	serviceName, foundName := h.GetGRPCServiceName(fullMethod)
	if !foundName {
		return true
	}
	componentSignature := h.GetComponentNameOnGRPC(ctx)
	serviceHandler, ok := h.ServiceHandlers[serviceName]
	if !ok {
		return true
	}
	limitAllow := serviceHandler.RateLimitAllow(componentSignature)
	if serviceHandler.EnableAudit() {
		logInfo := &LogInfo{
			ServiceName:    serviceName,
			Method:         fmt.Sprintf("gRPC:%s", fullMethod),
			Component:      componentSignature,
			TimeStamp:      time.Now().Local().String(),
			RateLimitAllow: limitAllow,
		}
		GetLogInfoFromGRPC(ctx, logInfo)
		serviceHandler.AuditLog(logInfo)
	}
	return limitAllow
}

type ServiceSelfProtectionHandler struct {
	apiRateLimiter *APIRateLimiter
	auditLogger    *AuditLogger
}

func NewServiceSelfProtectionHandler(config *config.ServiceSelfprotectionConfig) *ServiceSelfProtectionHandler {
	handler := &ServiceSelfProtectionHandler{}
	handler.Update(config)
	return handler
}

func (h *ServiceSelfProtectionHandler) Update(config *config.ServiceSelfprotectionConfig) {
	if h.apiRateLimiter == nil {
		h.apiRateLimiter = NewAPIRateLimiter(config)
	} else {
		h.apiRateLimiter.Update(config)
	}
	if h.auditLogger == nil {
		h.auditLogger = NewAuditLogger(config)
	} else {
		h.auditLogger.Update(config)
	}
}

func (h *ServiceSelfProtectionHandler) RateLimitAllow(componentName string) bool {
	if h.apiRateLimiter == nil {
		return true
	}
	return h.apiRateLimiter.Allow(componentName)
}

func (h *ServiceSelfProtectionHandler) EnableAudit() bool {
	if h.auditLogger == nil {
		return true
	}
	return h.auditLogger.Enable()
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
	h.auditLogger.Log(logInfo)
}

type APIRateLimiter struct {
	mu sync.RWMutex

	enableQPSLimit bool

	totalQPSRateLimiter *rate.Limiter

	enableComponentQPSLimit bool
	componentQPSRateLimiter map[string]*rate.Limiter
}

func NewAPIRateLimiter(config *config.ServiceSelfprotectionConfig) *APIRateLimiter {
	limiter := &APIRateLimiter{}
	limiter.Update(config)
	return limiter
}

func (rl *APIRateLimiter) Update(config *config.ServiceSelfprotectionConfig) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if config.TotalRateLimit > -1 {
		rl.enableQPSLimit = true
		rl.totalQPSRateLimiter = rate.NewLimiter(rate.Limit(config.TotalRateLimit), config.TotalRateLimit)
		if config.EnableComponentsLimit {
			rl.enableComponentQPSLimit = config.EnableComponentsLimit
			rl.componentQPSRateLimiter = make(map[string]*rate.Limiter)
			for _, item := range config.ComponentsRateLimits {
				component := item.Components
				limit := item.Limit
				rl.componentQPSRateLimiter[component] = rate.NewLimiter(rate.Limit(limit), limit)
			}
		}
	} else {
		rl.enableQPSLimit = false
	}
}

func (rl *APIRateLimiter) QPSAllow(componentName string) bool {
	if !rl.enableQPSLimit {
		return true
	}
	isComponentQPSLimit := true
	if rl.enableComponentQPSLimit {
		componentRateLimiter, ok := rl.componentQPSRateLimiter[componentName]
		if !ok {
			componentRateLimiter = rl.componentQPSRateLimiter[componentAnonymousValue]
		}
		isComponentQPSLimit = componentRateLimiter.Allow()
	}
	isTotalQPSLimit := rl.totalQPSRateLimiter.Allow()
	return isComponentQPSLimit && isTotalQPSLimit
}

func (rl *APIRateLimiter) Allow(componentName string) bool {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.QPSAllow(componentName)
}

type LogInfo struct {
	ServiceName    string
	Method         string
	Component      string
	IP             string
	TimeStamp      string
	Param          string
	RateLimitAllow bool
}

type AuditLogger struct {
	mu          sync.RWMutex
	enableAudit bool
	labels      map[string]bool
}

func NewAuditLogger(config *config.ServiceSelfprotectionConfig) *AuditLogger {
	logger := &AuditLogger{}
	logger.Update(config)
	return logger
}

func (logger *AuditLogger) Update(config *config.ServiceSelfprotectionConfig) {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(config.AuditLabel) == 0 {
		logger.enableAudit = false
	} else {
		logger.enableAudit = true
	}
}

func (logger *AuditLogger) Enable() bool {
	logger.mu.RLock()
	defer logger.mu.RUnlock()
	return logger.enableAudit
}

func (logger *AuditLogger) Log(info *LogInfo) {
	if isLog, ok := logger.labels[LoggerLabelLog]; ok {
		if isLog {
			log.Info("service_audit_detailed",
				zap.String("Service", info.ServiceName),
				zap.String("Method", info.Method),
				zap.String("Component", info.Component),
				zap.String("Param", info.Param),
				zap.String("TimeStamp", info.TimeStamp))
		}
	}
	if isMonitor, ok := logger.labels[LoggerLabelMonitored]; ok {
		if isMonitor {
			serviceAuditDetailed.WithLabelValues(info.ServiceName, info.Method, info.Component, info.IP, info.Param).SetToCurrentTime()
		}
	}
}

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
