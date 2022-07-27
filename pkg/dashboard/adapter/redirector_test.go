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

package adapter

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/pingcap/check"
)

func TestDashboardAdapter(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRedirectorSuite{})

type testRedirectorSuite struct {
	tempText   string
	tempServer *httptest.Server

	testName   string
	redirector *Redirector

	noRedirectHTTPClient *http.Client
}

func (s *testRedirectorSuite) SetUpSuite(c *C) {
	s.tempText = "temp1"
	s.tempServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, s.tempText)
	}))

	s.testName = "test1"
	s.redirector = NewRedirector(s.testName, nil)
	s.noRedirectHTTPClient = &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// ErrUseLastResponse can be returned by Client.CheckRedirect hooks to
			// control how redirects are processed. If returned, the next request
			// is not sent and the most recent response is returned with its body
			// unclosed.
			return http.ErrUseLastResponse
		},
	}
}

func (s *testRedirectorSuite) TearDownSuite(c *C) {
	s.tempServer.Close()
	s.noRedirectHTTPClient.CloseIdleConnections()
}

func (s *testRedirectorSuite) TestReverseProxy(c *C) {
	redirectorServer := httptest.NewServer(http.HandlerFunc(s.redirector.ReverseProxy))
	defer redirectorServer.Close()

	s.redirector.SetAddress(s.tempServer.URL)
	// Test normal forwarding
	req, err := http.NewRequest(http.MethodGet, redirectorServer.URL, nil)
	c.Assert(err, IsNil)
	checkHTTPRequest(c, s.noRedirectHTTPClient, req, http.StatusOK, s.tempText)
	// Test the requests that are forwarded by others
	req, err = http.NewRequest(http.MethodGet, redirectorServer.URL, nil)
	c.Assert(err, IsNil)
	req.Header.Set(proxyHeader, "other")
	checkHTTPRequest(c, s.noRedirectHTTPClient, req, http.StatusOK, s.tempText)
	// Test LoopDetected
	s.redirector.SetAddress(redirectorServer.URL)
	req, err = http.NewRequest(http.MethodGet, redirectorServer.URL, nil)
	c.Assert(err, IsNil)
	checkHTTPRequest(c, s.noRedirectHTTPClient, req, http.StatusLoopDetected, "")
}

func (s *testRedirectorSuite) TestTemporaryRedirect(c *C) {
	redirectorServer := httptest.NewServer(http.HandlerFunc(s.redirector.TemporaryRedirect))
	defer redirectorServer.Close()
	s.redirector.SetAddress(s.tempServer.URL)
	// Test TemporaryRedirect
	req, err := http.NewRequest(http.MethodGet, redirectorServer.URL, nil)
	c.Assert(err, IsNil)
	checkHTTPRequest(c, s.noRedirectHTTPClient, req, http.StatusTemporaryRedirect, "")
	// Test Response
	req, err = http.NewRequest(http.MethodGet, redirectorServer.URL, nil)
	c.Assert(err, IsNil)
	checkHTTPRequest(c, http.DefaultClient, req, http.StatusOK, s.tempText)
}

func checkHTTPRequest(c *C, client *http.Client, req *http.Request, expectedCode int, expectedText string) {
	resp, err := client.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, expectedCode)
	if expectedCode >= http.StatusOK && expectedCode <= http.StatusAlreadyReported {
		text, err := io.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		c.Assert(string(text), Equals, expectedText)
	}
}
