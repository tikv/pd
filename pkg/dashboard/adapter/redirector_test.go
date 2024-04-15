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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type redirectorTestSuite struct {
	suite.Suite

	tempText   string
	tempServer *httptest.Server

	testName   string
	redirector *Redirector

	noRedirectHTTPClient *http.Client
}

func TestRedirectorTestSuite(t *testing.T) {
	suite.Run(t, new(redirectorTestSuite))
}

func (suite *redirectorTestSuite) SetupSuite() {
	suite.tempText = "temp1"
	suite.tempServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, suite.tempText)
	}))

	suite.testName = "test1"
	suite.redirector = NewRedirector(suite.testName, nil)
	suite.noRedirectHTTPClient = &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			// ErrUseLastResponse can be returned by Client.CheckRedirect hooks to
			// control how redirects are processed. If returned, the next request
			// is not sent and the most recent response is returned with its body
			// unclosed.
			return http.ErrUseLastResponse
		},
	}
}

func (suite *redirectorTestSuite) TearDownSuite() {
	suite.tempServer.Close()
	suite.noRedirectHTTPClient.CloseIdleConnections()
}

func (suite *redirectorTestSuite) TestReverseProxy() {
	re := suite.Require()
	redirectorServer := httptest.NewServer(http.HandlerFunc(suite.redirector.ReverseProxy))
	defer redirectorServer.Close()

	suite.redirector.SetAddress(suite.tempServer.URL)
	// Test normal forwarding
	req, err := http.NewRequest(http.MethodGet, redirectorServer.URL, http.NoBody)
	re.NoError(err)
	checkHTTPRequest(re, suite.noRedirectHTTPClient, req, http.StatusOK, suite.tempText)
	// Test the requests that are forwarded by others
	req, err = http.NewRequest(http.MethodGet, redirectorServer.URL, http.NoBody)
	re.NoError(err)
	req.Header.Set(proxyHeader, "other")
	checkHTTPRequest(re, suite.noRedirectHTTPClient, req, http.StatusOK, suite.tempText)
	// Test LoopDetected
	suite.redirector.SetAddress(redirectorServer.URL)
	req, err = http.NewRequest(http.MethodGet, redirectorServer.URL, http.NoBody)
	re.NoError(err)
	checkHTTPRequest(re, suite.noRedirectHTTPClient, req, http.StatusLoopDetected, "")
}

func (suite *redirectorTestSuite) TestTemporaryRedirect() {
	re := suite.Require()
	redirectorServer := httptest.NewServer(http.HandlerFunc(suite.redirector.TemporaryRedirect))
	defer redirectorServer.Close()
	suite.redirector.SetAddress(suite.tempServer.URL)
	// Test TemporaryRedirect
	req, err := http.NewRequest(http.MethodGet, redirectorServer.URL, http.NoBody)
	re.NoError(err)
	checkHTTPRequest(re, suite.noRedirectHTTPClient, req, http.StatusTemporaryRedirect, "")
	// Test Response
	req, err = http.NewRequest(http.MethodGet, redirectorServer.URL, http.NoBody)
	re.NoError(err)
	checkHTTPRequest(re, http.DefaultClient, req, http.StatusOK, suite.tempText)
}

func checkHTTPRequest(re *require.Assertions, client *http.Client, req *http.Request, expectedCode int, expectedText string) {
	resp, err := client.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(expectedCode, resp.StatusCode)
	if expectedCode >= http.StatusOK && expectedCode <= http.StatusAlreadyReported {
		text, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.Equal(expectedText, string(text))
	}
}
