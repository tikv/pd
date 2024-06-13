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

package apiutil

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/pingcap/errcode"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/slice"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

const (
	// componentSignatureKey is used for http request header key to identify component signature.
	// Deprecated: please use `XCallerIDHeader` below to obtain a more granular source identification.
	// This is kept for backward compatibility.
	componentSignatureKey = "component"
	// anonymousValue identifies anonymous request source
	anonymousValue = "anonymous"

	// PDRedirectorHeader is used to mark which PD redirected this request.
	PDRedirectorHeader = "PD-Redirector"
	// PDAllowFollowerHandleHeader is used to mark whether this request is allowed to be handled by the follower PD.
	PDAllowFollowerHandleHeader = "PD-Allow-follower-handle" // #nosec G101
	// XForwardedForHeader is used to mark the client IP.
	XForwardedForHeader = "X-Forwarded-For"
	// XForwardedPortHeader is used to mark the client port.
	XForwardedPortHeader = "X-Forwarded-Port"
	// XRealIPHeader is used to mark the real client IP.
	XRealIPHeader = "X-Real-Ip"
	// XCallerIDHeader is used to mark the caller ID.
	XCallerIDHeader = "X-Caller-ID"
	// XForbiddenForwardToMicroServiceHeader is used to indicate that forwarding the request to a microservice is explicitly disallowed.
	XForbiddenForwardToMicroServiceHeader = "X-Forbidden-Forward-To-MicroService"
	// XForwardedToMicroServiceHeader is used to signal that the request has already been forwarded to a microservice.
	XForwardedToMicroServiceHeader = "X-Forwarded-To-MicroService"

	chunkSize = 4096
)

var once sync.Once

func init() {
	once.Do(func() {
		// See https://github.com/pingcap/tidb-dashboard/blob/f8ecb64e3d63f4ed91c3dca7a04362418ade01d8/pkg/apiserver/apiserver.go#L84
		// These global modification will be effective only for the first invoke.
		_ = godotenv.Load()
		gin.SetMode(gin.ReleaseMode)
	})
}

// DeferClose captures the error returned from closing (if an error occurs).
// This is designed to be used in a defer statement.
func DeferClose(c io.Closer, err *error) {
	if cerr := c.Close(); cerr != nil && *err == nil {
		*err = errors.WithStack(cerr)
	}
}

// JSONError lets callers check for just one error type
type JSONError struct {
	Err error
}

func (e JSONError) Error() string {
	return e.Err.Error()
}

// TagJSONError wraps the JSON error to one type.
func TagJSONError(err error) error {
	switch err.(type) {
	case *json.SyntaxError, *json.UnmarshalTypeError:
		return JSONError{err}
	}
	return err
}

// ErrorResp Respond to the client about the given error, integrating with errcode.ErrorCode.
//
// Important: if the `err` is just an error and not an errcode.ErrorCode (given by errors.Cause),
// then by default an error is assumed to be a 500 Internal Error.
//
// If the error is nil, this also responds with a 500 and logs at the error level.
func ErrorResp(rd *render.Render, w http.ResponseWriter, err error) {
	if err == nil {
		log.Error("nil is given to errorResp")
		rd.JSON(w, http.StatusInternalServerError, "nil error")
		return
	}
	if errCode := errcode.CodeChain(err); errCode != nil {
		w.Header().Set("TiDB-Error-Code", errCode.Code().CodeStr().String())
		rd.JSON(w, errCode.Code().HTTPCode(), errcode.NewJSONFormat(errCode))
	} else {
		rd.JSON(w, http.StatusInternalServerError, err.Error())
	}
}

// GetIPPortFromHTTPRequest returns http client host IP and port from context.
// Because `X-Forwarded-For ` header has been written into RFC 7239(Forwarded HTTP Extension),
// so `X-Forwarded-For` has the higher priority than `X-Real-Ip`.
// And both of them have the higher priority than `RemoteAddr`
func GetIPPortFromHTTPRequest(r *http.Request) (ip, port string) {
	forwardedIPs := strings.Split(r.Header.Get(XForwardedForHeader), ",")
	if forwardedIP := strings.Trim(forwardedIPs[0], " "); len(forwardedIP) > 0 {
		ip = forwardedIP
		// Try to get the port from "X-Forwarded-Port" header.
		forwardedPorts := strings.Split(r.Header.Get(XForwardedPortHeader), ",")
		if forwardedPort := strings.Trim(forwardedPorts[0], " "); len(forwardedPort) > 0 {
			port = forwardedPort
		}
	} else if realIP := r.Header.Get(XRealIPHeader); len(realIP) > 0 {
		ip = realIP
	} else {
		ip = r.RemoteAddr
	}
	splitIP, splitPort, err := net.SplitHostPort(ip)
	if err != nil {
		// Ensure we could get an IP address at least.
		return ip, port
	}
	return splitIP, splitPort
}

// getComponentNameOnHTTP returns component name from the request header.
func getComponentNameOnHTTP(r *http.Request) string {
	componentName := r.Header.Get(componentSignatureKey)
	if len(componentName) == 0 {
		componentName = anonymousValue
	}
	return componentName
}

// GetCallerIDOnHTTP returns caller ID from the request header.
func GetCallerIDOnHTTP(r *http.Request) string {
	callerID := r.Header.Get(XCallerIDHeader)
	if len(callerID) == 0 {
		// Fall back to get the component name to keep backward compatibility.
		callerID = getComponentNameOnHTTP(r)
	}
	return callerID
}

// CallerIDRoundTripper is used to add caller ID in the HTTP header.
type CallerIDRoundTripper struct {
	proxied  http.RoundTripper
	callerID string
}

// NewCallerIDRoundTripper returns a new `CallerIDRoundTripper`.
func NewCallerIDRoundTripper(roundTripper http.RoundTripper, callerID string) *CallerIDRoundTripper {
	return &CallerIDRoundTripper{
		proxied:  roundTripper,
		callerID: callerID,
	}
}

// RoundTrip is used to implement RoundTripper
func (rt *CallerIDRoundTripper) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	req.Header.Add(XCallerIDHeader, rt.callerID)
	// Send the request, get the response and the error
	resp, err = rt.proxied.RoundTrip(req)
	return
}

// GetRouteName return mux route name registered
func GetRouteName(req *http.Request) string {
	route := mux.CurrentRoute(req)
	if route != nil {
		return route.GetName()
	}
	return ""
}

// AccessPath is used to identify HTTP api access path including path and method
type AccessPath struct {
	Path   string
	Method string
}

// NewAccessPath returns an AccessPath
func NewAccessPath(path, method string) AccessPath {
	return AccessPath{Path: path, Method: method}
}

// PostJSON is used to send the POST request to a specific URL
func PostJSON(client *http.Client, url string, data []byte) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return client.Do(req)
}

// GetJSON is used to send GET request to specific url
func GetJSON(client *http.Client, url string, data []byte) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

// PatchJSON is used to do patch request
func PatchJSON(client *http.Client, url string, data []byte) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

// PostJSONIgnoreResp is used to do post request with JSON body and ignore response.
func PostJSONIgnoreResp(client *http.Client, url string, data []byte) error {
	resp, err := PostJSON(client, url, data)
	return checkResponse(resp, err)
}

// DoDelete is used to send delete request and return http response code.
func DoDelete(client *http.Client, url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodDelete, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func checkResponse(resp *http.Response, err error) error {
	if err != nil {
		return errors.WithStack(err)
	}
	res, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(string(res))
	}
	return nil
}

// FieldError connects an error to a particular field
type FieldError struct {
	error
	field string
}

// ParseUint64VarsField connects strconv.ParseUint with request variables
// It hardcodes the base to 10 and bit size to 64
// Any error returned will connect the requested field to the error via FieldError
func ParseUint64VarsField(vars map[string]string, varName string) (uint64, *FieldError) {
	str, ok := vars[varName]
	if !ok {
		return 0, &FieldError{field: varName, error: fmt.Errorf("field %s not present", varName)}
	}
	parsed, err := strconv.ParseUint(str, 10, 64)
	if err == nil {
		return parsed, nil
	}
	return parsed, &FieldError{field: varName, error: err}
}

// CollectEscapeStringOption is used to collect string using escaping from input map for given option
func CollectEscapeStringOption(option string, input map[string]any, collectors ...func(v string)) error {
	if v, ok := input[option].(string); ok {
		value, err := url.QueryUnescape(v)
		if err != nil {
			return err
		}
		for _, c := range collectors {
			c(value)
		}
		return nil
	}
	return errs.ErrOptionNotExist.FastGenByArgs(option)
}

// CollectStringOption is used to collect string using from input map for given option
func CollectStringOption(option string, input map[string]any, collectors ...func(v string)) error {
	if v, ok := input[option].(string); ok {
		for _, c := range collectors {
			c(v)
		}
		return nil
	}
	return errs.ErrOptionNotExist.FastGenByArgs(option)
}

// ParseKey is used to parse interface into []byte and string
func ParseKey(name string, input map[string]any) ([]byte, string, error) {
	k, ok := input[name]
	if !ok {
		return nil, "", fmt.Errorf("missing %s", name)
	}
	rawKey, ok := k.(string)
	if !ok {
		return nil, "", fmt.Errorf("bad format %s", name)
	}
	returned, err := hex.DecodeString(rawKey)
	if err != nil {
		return nil, "", fmt.Errorf("split key %s is not in hex format", name)
	}
	return returned, rawKey, nil
}

// ParseHexKeys decodes hexadecimal src into DecodedLen(len(src)) bytes if the format is "hex".
//
// ParseHexKeys expects that each key contains only
// hexadecimal characters and each key has even length.
// If existing one key is malformed, ParseHexKeys returns
// the original bytes.
func ParseHexKeys(format string, keys [][]byte) (decodedBytes [][]byte, err error) {
	if format != "hex" {
		return keys, nil
	}

	for _, key := range keys {
		// We can use the source slice itself as the destination
		// because the decode loop increments by one and then the 'seen' byte is not used anymore.
		// Reference to hex.DecodeString()
		n, err := hex.Decode(key, key)
		if err != nil {
			return keys, err
		}
		decodedBytes = append(decodedBytes, key[:n])
	}
	return decodedBytes, nil
}

// ReadJSON reads a JSON data from r and then closes it.
// An error due to invalid json will be returned as a JSONError
func ReadJSON(r io.ReadCloser, data any) error {
	var err error
	defer DeferClose(r, &err)
	b, err := io.ReadAll(r)
	if err != nil {
		return errors.WithStack(err)
	}

	err = json.Unmarshal(b, data)
	if err != nil {
		return TagJSONError(err)
	}

	return err
}

// ReadJSONRespondError writes json into data.
// On error respond with a 400 Bad Request
func ReadJSONRespondError(rd *render.Render, w http.ResponseWriter, body io.ReadCloser, data any) error {
	err := ReadJSON(body, data)
	if err == nil {
		return nil
	}
	var errCode errcode.ErrorCode
	if jsonErr, ok := errors.Cause(err).(JSONError); ok {
		errCode = errcode.NewInvalidInputErr(jsonErr.Err)
	} else {
		errCode = errcode.NewInternalErr(err)
	}
	ErrorResp(rd, w, errCode)
	return err
}

const (
	// CorePath the core group, is at REST path `/pd/api/v1`.
	CorePath = "/pd/api/v1"
	// ExtensionsPath the named groups are REST at `/pd/apis/{GROUP_NAME}/{Version}`.
	ExtensionsPath = "/pd/apis"
)

// APIServiceGroup used to register the HTTP REST API.
type APIServiceGroup struct {
	Name       string
	Version    string
	IsCore     bool
	PathPrefix string
}

// Path returns the path of the service.
func (sg *APIServiceGroup) Path() string {
	if len(sg.PathPrefix) > 0 {
		return sg.PathPrefix
	}
	if sg.IsCore {
		return CorePath
	}
	if len(sg.Name) > 0 && len(sg.Version) > 0 {
		return path.Join(ExtensionsPath, sg.Name, sg.Version)
	}
	return ""
}

// RegisterUserDefinedHandlers register the user defined handlers.
func RegisterUserDefinedHandlers(registerMap map[string]http.Handler, group *APIServiceGroup, handler http.Handler) error {
	pathPrefix := group.Path()
	if _, ok := registerMap[pathPrefix]; ok {
		return errs.ErrServiceRegistered.FastGenByArgs(pathPrefix)
	}
	if len(pathPrefix) == 0 {
		return errs.ErrAPIInformationInvalid.FastGenByArgs(group.Name, group.Version)
	}
	registerMap[pathPrefix] = handler
	log.Info("register REST path", zap.String("path", pathPrefix))
	return nil
}

type customReverseProxies struct {
	urls   []url.URL
	client *http.Client
}

// NewCustomReverseProxies returns the custom reverse proxies.
func NewCustomReverseProxies(dialClient *http.Client, urls []url.URL) http.Handler {
	p := &customReverseProxies{
		client: dialClient,
	}

	p.urls = append(p.urls, urls...)

	return p
}

func (p *customReverseProxies) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for _, url := range p.urls {
		r.RequestURI = ""
		r.URL.Host = url.Host
		r.URL.Scheme = url.Scheme

		resp, err := p.client.Do(r)
		if err != nil {
			log.Error("request failed", errs.ZapError(errs.ErrSendRequest, err))
			continue
		}
		var reader io.ReadCloser
		switch resp.Header.Get("Content-Encoding") {
		case "gzip":
			reader, err = gzip.NewReader(resp.Body)
			if err != nil {
				log.Error("failed to parse response with gzip compress", zap.Error(err))
				resp.Body.Close()
				continue
			}
		default:
			reader = resp.Body
		}

		// We need to copy the response headers before we write the header.
		// Otherwise, we cannot set the header after w.WriteHeader() is called.
		// And we need to write the header before we copy the response body.
		// Otherwise, we cannot set the status code after w.Write() is called.
		// In other words, we must perform the following steps strictly in order:
		// 1. Set the response headers.
		// 2. Write the response header.
		// 3. Write the response body.
		copyHeader(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)

		for {
			if _, err = io.CopyN(w, reader, chunkSize); err != nil {
				if err == io.EOF {
					err = nil
				}
				break
			}
		}
		resp.Body.Close()
		reader.Close()
		if err != nil {
			log.Error("write failed", errs.ZapError(errs.ErrWriteHTTPBody, err), zap.String("target-address", url.String()))
			// try next url.
			continue
		}
		return
	}
	http.Error(w, errs.ErrRedirect.FastGenByArgs().Error(), http.StatusInternalServerError)
}

// copyHeader duplicates the HTTP headers from the source `src` to the destination `dst`.
// It skips the "Content-Encoding" and "Content-Length" headers because they should be set by `http.ResponseWriter`.
// These headers may be modified after a redirect when gzip compression is enabled.
func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		if k == "Content-Encoding" || k == "Content-Length" {
			continue
		}
		values := dst[k]
		for _, v := range vv {
			if !slice.Contains(values, v) {
				dst.Add(k, v)
			}
		}
	}
}

// ParseTime parses a time string with the format "1694580288"
// If the string is empty, it returns a zero time.
func ParseTime(t string) (time.Time, error) {
	if len(t) == 0 {
		return time.Time{}, nil
	}
	i, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(i, 0), nil
}
