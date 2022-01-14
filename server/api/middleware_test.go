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
	"net/http"
	"testing"

	"github.com/tikv/pd/pkg/requestutil"
	"github.com/tikv/pd/server/config"
)

func BenchmarkDoRequestWithoutServiceInfo(b *testing.B) {
	b.StopTimer()
	registeredSericeLabel := requestutil.NewRequestSchemaList(len(config.HTTPRegisteredSericeLabel))
	length := len(config.HTTPRegisteredSericeLabel)
	reqList := make([]*http.Request, 0, length)
	for key, _ := range config.HTTPRegisteredSericeLabel {
		pos := len(key)
		for ; key[pos-1] != '/'; pos-- {
		}
		var method string
		if pos == len(key) {
			method = "GET"
		} else {
			method = key[pos:]
		}

		newReq, _ := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1%s", key[:pos-1]), nil)
		reqList = append(reqList, newReq)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		registeredSericeLabel.GetRequestInfo(reqList[i%length])
	}
}
