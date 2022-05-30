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

package apiutil

import (
	"bytes"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unrolled/render"
)

func TestJsonRespondErrorOk(t *testing.T) {
	rd := render.New(render.Options{
		IndentJSON: true,
	})
	response := httptest.NewRecorder()
	body := io.NopCloser(bytes.NewBufferString("{\"zone\":\"cn\", \"host\":\"local\"}"))
	var input map[string]string
	output := map[string]string{"zone": "cn", "host": "local"}
	err := ReadJSONRespondError(rd, response, body, &input)
	require.NoError(t, err)
	require.Equal(t, output["zone"], input["zone"])
	require.Equal(t, output["host"], input["host"])
	result := response.Result()
	defer result.Body.Close()
	require.Equal(t, 200, result.StatusCode)
}

func TestJsonRespondErrorBadInput(t *testing.T) {
	rd := render.New(render.Options{
		IndentJSON: true,
	})
	response := httptest.NewRecorder()
	body := io.NopCloser(bytes.NewBufferString("{\"zone\":\"cn\", \"host\":\"local\"}"))
	var input []string
	err := ReadJSONRespondError(rd, response, body, &input)
	require.EqualError(t, err, "json: cannot unmarshal object into Go value of type []string")
	result := response.Result()
	defer result.Body.Close()
	require.Equal(t, 400, result.StatusCode)

	{
		body := io.NopCloser(bytes.NewBufferString("{\"zone\":\"cn\","))
		var input []string
		err := ReadJSONRespondError(rd, response, body, &input)
		require.EqualError(t, err, "unexpected end of JSON input")
		result := response.Result()
		defer result.Body.Close()
		require.Equal(t, 400, result.StatusCode)
	}
}
