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

// SelfProtectionHandler is a framework to handle self protection mechanism
// Self-protection granularity is a logical service
type SelfProtectionHandler struct {
	// grpcServiceNames is used to find the service name of grpc method
	GrpcServiceNames map[string]string
	// ServiceHandlers a
	ServiceHandlers map[string]*ServiceSelfProtectionHandler
}

// MergeSelfProtectionConfig is used for when both the user configuration and the default configuration exist
func MergeSelfProtectionConfig(handlers map[string]*ServiceSelfProtectionHandler, highPriorityConfigs []config.ServiceSelfprotectionConfig, lowPriorityConfigs []config.ServiceSelfprotectionConfig) {
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

// GetHTTPAPIServiceName return mux route name registered for ServiceName
func (h *SelfProtectionHandler) GetHTTPAPIServiceName(req *http.Request) (string, bool) {
	route := mux.CurrentRoute(req)
	if route != nil {
		if route.GetName() != "" {
			return route.GetName(), true
		}
	}
	return "", false
}

// GetGRPCServiceName return ServiceName by mapping gRPC method name
func (h *SelfProtectionHandler) GetGRPCServiceName(method string) (string, bool) {
	serviceName, ok := h.GrpcServiceNames[method]
	return serviceName, ok
}

// GetComponentNameOnHTTP return component name from Request Header
func (h *SelfProtectionHandler) GetComponentNameOnHTTP(r *http.Request) string {
	componentName := r.Header.Get(componentSignatureKey)
	if componentName == "" {
		componentName = componentAnonymousValue
	}
	return componentName
}

// GetComponentNameOnGRPC return component name from gRPC metadata
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

// SelfProtectionHandleHTTP is used to handle http api self protection
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

// SelfProtectionHandleGRPC is used to handle gRPC self protection
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

// ServiceSelfProtectionHandler currently includes QPS rate limiter and audit logger
type ServiceSelfProtectionHandler struct {
	apiRateLimiter *APIRateLimiter
	auditLogger    *AuditLogger
}

// NewServiceSelfProtectionHandler return a new ServiceSelfProtectionHandler
func NewServiceSelfProtectionHandler(config *config.ServiceSelfprotectionConfig) *ServiceSelfProtectionHandler {
	handler := &ServiceSelfProtectionHandler{}
	handler.Update(config)
	return handler
}

// Update is used to update ServiceSelfProtectionHandler
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

// RateLimitAllow is used to check whether the rate limit allow request process
func (h *ServiceSelfProtectionHandler) RateLimitAllow(componentName string) bool {
	if h.apiRateLimiter == nil {
		return true
	}
	return h.apiRateLimiter.Allow(componentName)
}

// EnableAudit is used to check Whether to enable the audit handle
func (h *ServiceSelfProtectionHandler) EnableAudit() bool {
	if h.auditLogger == nil {
		return true
	}
	return h.auditLogger.Enable()
}

// GetLogInfoFromHTTP return LogInfo from http.Request
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

// GetLogInfoFromHTTP return LogInfo from Context
func GetLogInfoFromGRPC(ctx context.Context, logInfo *LogInfo) {
	// Get IP
	logInfo.IP = apiutil.GetIPAddrFromGRPCContext(ctx)
	// gRPC can't get Param in middware
}

// AuditLog is a entrance to access AuditLoggor
func (h *ServiceSelfProtectionHandler) AuditLog(logInfo *LogInfo) {
	h.auditLogger.Log(logInfo)
}

// APIRateLimiter is used to limit unnecessary and excess request
// Currently support QPS rate limit by compoenent
// It depends on the rate.Limiter which implement a token-bucket algorithm
type APIRateLimiter struct {
	mu sync.RWMutex

	enableQPSLimit bool

	totalQPSRateLimiter *rate.Limiter

	enableComponentQPSLimit bool
	componentQPSRateLimiter map[string]*rate.Limiter
}

// NewAPIRateLimiter create a new api rate limiter
func NewAPIRateLimiter(config *config.ServiceSelfprotectionConfig) *APIRateLimiter {
	limiter := &APIRateLimiter{}
	limiter.Update(config)
	return limiter
}

// Update will replace all handler by service
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

// QPSAllow firstly check component token bucket and then check total token bucket
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

// Allow currentlt only supports QPS rate limit
func (rl *APIRateLimiter) Allow(componentName string) bool {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.QPSAllow(componentName)
}

// LogInfo stores needed api request info
type LogInfo struct {
	ServiceName    string
	Method         string
	Component      string
	IP             string
	TimeStamp      string
	Param          string
	RateLimitAllow bool
}

// AuditLogger is used to record some information about the service for auditing when problems occur
// Currently it can be bonded two audit labels
// LoggerLabelLog("log") means AuditLogger will restore info in local file system
// LoggerLabelMonitored("Monitored") means AuditLogger will report info to promethus
type AuditLogger struct {
	mu          sync.RWMutex
	enableAudit bool
	labels      map[string]bool
}

// NewAuditLogger return a new AuditLogger
func NewAuditLogger(config *config.ServiceSelfprotectionConfig) *AuditLogger {
	logger := &AuditLogger{}
	logger.Update(config)
	return logger
}

// Update AuditLogger by config
func (logger *AuditLogger) Update(config *config.ServiceSelfprotectionConfig) {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(config.AuditLabel) == 0 {
		logger.enableAudit = false
	} else {
		logger.enableAudit = true
	}
}

// Enable is used to check Whether to enable the audit handle
func (logger *AuditLogger) Enable() bool {
	logger.mu.RLock()
	defer logger.mu.RUnlock()
	return logger.enableAudit
}

// Log is used to handle log action
func (logger *AuditLogger) Log(info *LogInfo) {
	logger.mu.RLock()
	defer logger.mu.RUnlock()
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

// UnaryServerInterceptor returns a gRPC stream server interceptor to handle self protection in gRPC unary service
func (h *SelfProtectionHandler) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return grpc.UnaryServerInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		if h.SelfProtectionHandleGRPC(info.FullMethod, ctx) {
			return nil, status.Errorf(codes.ResourceExhausted, "%s is rejected by grpc_ratelimit middleware, please retry later.", info.FullMethod)
		}
		return handler(ctx, req)
	})
}

// StreamServerInterceptor returns a gRPC stream server interceptor to handle self protection in gRPC stream service
func (h *SelfProtectionHandler) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if h.SelfProtectionHandleGRPC(info.FullMethod, stream.Context()) {
			return status.Errorf(codes.ResourceExhausted, "%s is rejected by grpc_ratelimit middleware, please retry later.", info.FullMethod)
		}
		return handler(srv, stream)
	}
}

// ComponentSignatureGRPCClientInterceptorBuilder add component signature in gRPC
type ComponentSignatureGRPCClientInterceptorBuilder struct {
	component string
}

// SetComponentName set component name
func (builder *ComponentSignatureGRPCClientInterceptorBuilder) SetComponentName(component string) {
	builder.component = component
}

// UnaryClientInterceptor return a ComponentSignature UnaryClientInterceptor
func (builder *ComponentSignatureGRPCClientInterceptorBuilder) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		headerData := metadata.Pairs(componentSignatureKey, builder.component)
		ctxH := metadata.NewOutgoingContext(ctx, headerData)
		err := invoker(ctxH, method, req, reply, cc, opts...)
		return err
	}
}

// StreamClientInterceptor return a ComponentSignature StreamClientInterceptor
func (builder *ComponentSignatureGRPCClientInterceptorBuilder) StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		headerData := metadata.Pairs(componentSignatureKey, builder.component)
		ctxH := metadata.NewOutgoingContext(ctx, headerData)
		return streamer(ctxH, desc, cc, method, opts...)
	}
}

// UserSignatureDialOptions create opts with ComponentSignature Interceptors
func (builder *ComponentSignatureGRPCClientInterceptorBuilder) UserSignatureDialOptions() []grpc.DialOption {
	streamInterceptors := []grpc.StreamClientInterceptor{builder.StreamClientInterceptor()}
	unaryInterceptors := []grpc.UnaryClientInterceptor{builder.UnaryClientInterceptor()}
	opts := []grpc.DialOption{grpc.WithChainStreamInterceptor(streamInterceptors...), grpc.WithChainUnaryInterceptor(unaryInterceptors...)}
	return opts
}

// UserSignatureRoundTripper add component user signature in http
type UserSignatureRoundTripper struct {
	Proxied   http.RoundTripper
	Component string
}

// RoundTrip is used to implement RoundTripper
func (rt *UserSignatureRoundTripper) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	req.Header.Set(componentSignatureKey, rt.Component)
	// Send the request, get the response and the error
	resp, err = rt.Proxied.RoundTrip(req)
	return
}
