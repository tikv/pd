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

package api

import (
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tikv/pd/pkg/requestutil"
	"github.com/tikv/pd/server/config"
)

var ra *rand.Rand = rand.New(rand.NewSource(time.Now().Unix()))

func BenchmarkDoServiceLabel(b *testing.B) {
	b.StopTimer()
	registeredSericeLabel := requestutil.NewRequestSchemaList(len(config.HTTPRegisteredServiceLabel))
	for key, value := range config.HTTPRegisteredServiceLabel {
		if key[0] != '/' {
			continue
		}
		paths := strings.Split(key, "/")
		length := len(paths)
		registeredSericeLabel.AddServiceLabel(paths[1:length-1], paths[length-1], value)
	}

	length := len(config.HTTPRegisteredServiceLabel)
	reqList := make([]*http.Request, 0, length)
	for key := range config.HTTPRegisteredServiceLabel {
		pos := len(key)
		for ; key[pos-1] != '/'; pos-- {
		}
		var method string
		if pos == len(key) {
			method = "GET"
		} else {
			method = key[pos:]
		}

		newReq, _ := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1%s", key[:pos-1]), strings.NewReader(randStr()))
		reqList = append(reqList, newReq)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		registeredSericeLabel.GetServiceLabel(reqList[i%length])
	}
}

func BenchmarkDoRequestInfo(b *testing.B) {
	b.StopTimer()
	registeredSericeLabel := requestutil.NewRequestSchemaList(len(config.HTTPRegisteredServiceLabel))
	for key, value := range config.HTTPRegisteredServiceLabel {
		if key[0] != '/' {
			continue
		}
		paths := strings.Split(key, "/")
		length := len(paths)
		registeredSericeLabel.AddServiceLabel(paths[1:length-1], paths[length-1], value)
	}

	length := len(config.HTTPRegisteredServiceLabel)
	reqList := make([]*http.Request, 0, length)
	for key := range config.HTTPRegisteredServiceLabel {
		pos := len(key)
		for ; key[pos-1] != '/'; pos-- {
		}
		var method string
		if pos == len(key) {
			method = "GET"
		} else {
			method = key[pos:]
		}

		newReq, _ := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1%s", key[:pos-1]), strings.NewReader(randStr()))
		reqList = append(reqList, newReq)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		registeredSericeLabel.GetRequestInfo(reqList[i%length])
	}
}

func randStr() string {
	length := int(ra.Int31n(80))
	bytes := make([]byte, length)
	for i := 0; i < length; i++ {
		b := ra.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}
