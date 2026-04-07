// Copyright 2025 TiKV Project Authors.
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

package command

import (
	"bytes"
	"encoding/binary"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/codec"
)

type captureRegionRoundTripper struct {
	path     string
	rawQuery string
	body     string
	err      error
}

func (m *captureRegionRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	m.path = req.URL.Path
	m.rawQuery = req.URL.RawQuery
	if m.err != nil {
		return nil, m.err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(m.body)),
	}, nil
}

func TestRegionKeyspaceIDPath(t *testing.T) {
	re := require.New(t)
	rt := &captureRegionRoundTripper{body: `{"ok":true}`}
	oldClient := dialClient
	dialClient = &http.Client{Transport: rt}
	defer func() { dialClient = oldClient }()

	cmd := NewRegionWithKeyspaceCommand()
	cmd.PersistentFlags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"id", "1"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)

	re.NoError(cmd.Execute())
	re.Equal("/pd/api/v1/regions/keyspace/id/1", rt.path)
	re.Empty(rt.rawQuery)
	re.Contains(out.String(), `{"ok":true}`)
}

func TestRegionKeyspaceIDTableIDPath(t *testing.T) {
	re := require.New(t)
	rt := &captureRegionRoundTripper{body: `{"ok":true}`}
	oldClient := dialClient
	dialClient = &http.Client{Transport: rt}
	defer func() { dialClient = oldClient }()

	startKey, endKey := expectedTableRangeInKeyspace(1, 100)
	query := make(url.Values)
	query.Set("key", string(startKey))
	query.Set("end_key", string(endKey))

	cmd := NewRegionWithKeyspaceCommand()
	cmd.PersistentFlags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"id", "1", "table-id", "100"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)

	re.NoError(cmd.Execute())
	re.Equal("/pd/api/v1/regions/key", rt.path)
	re.Equal(query.Encode(), rt.rawQuery)
	re.Contains(out.String(), `{"ok":true}`)
}

func TestRegionKeyspaceIDPathWithLimit(t *testing.T) {
	re := require.New(t)
	rt := &captureRegionRoundTripper{body: `{"ok":true}`}
	oldClient := dialClient
	dialClient = &http.Client{Transport: rt}
	defer func() { dialClient = oldClient }()

	cmd := NewRegionWithKeyspaceCommand()
	cmd.PersistentFlags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"id", "1", "16"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)

	re.NoError(cmd.Execute())
	re.Equal("/pd/api/v1/regions/keyspace/id/1", rt.path)
	re.Equal("limit=16", rt.rawQuery)
	re.Contains(out.String(), `{"ok":true}`)
}

func TestRegionKeyspaceIDTableIDPathWithLimit(t *testing.T) {
	re := require.New(t)
	rt := &captureRegionRoundTripper{body: `{"ok":true}`}
	oldClient := dialClient
	dialClient = &http.Client{Transport: rt}
	defer func() { dialClient = oldClient }()

	startKey, endKey := expectedTableRangeInKeyspace(1, 100)
	query := make(url.Values)
	query.Set("key", string(startKey))
	query.Set("end_key", string(endKey))
	query.Set("limit", "16")

	cmd := NewRegionWithKeyspaceCommand()
	cmd.PersistentFlags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"id", "1", "table-id", "100", "16"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)

	re.NoError(cmd.Execute())
	re.Equal("/pd/api/v1/regions/key", rt.path)
	re.Equal(query.Encode(), rt.rawQuery)
	re.Contains(out.String(), `{"ok":true}`)
}

func TestRegionKeyspaceIDInvalidKeyspaceID(t *testing.T) {
	re := require.New(t)

	cmd := NewRegionWithKeyspaceCommand()
	cmd.PersistentFlags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"id", "invalid"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)

	re.NoError(cmd.Execute())
	re.Contains(out.String(), "keyspace_id should be a number")
}

func TestRegionKeyspaceIDOutOfRangeKeyspaceID(t *testing.T) {
	re := require.New(t)

	cmd := NewRegionWithKeyspaceCommand()
	cmd.PersistentFlags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"id", "16777216"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)

	re.NoError(cmd.Execute())
	re.Contains(out.String(), "invalid keyspace id 16777216. It must be in the range of [0, 16777215]")
}

func TestRegionKeyspaceIDInvalidTableID(t *testing.T) {
	re := require.New(t)

	cmd := NewRegionWithKeyspaceCommand()
	cmd.PersistentFlags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"id", "1", "table-id", "invalid"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)

	re.NoError(cmd.Execute())
	re.Contains(out.String(), "table-id should be a number")
}

func TestRegionKeyspaceIDInvalidLimit(t *testing.T) {
	re := require.New(t)

	cmd := NewRegionWithKeyspaceCommand()
	cmd.PersistentFlags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"id", "1", "table-id", "100", "invalid"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)

	re.NoError(cmd.Execute())
	re.Contains(out.String(), "limit should be a number")
}

func TestRegionKeyspaceIDWrongTableIDLiteral(t *testing.T) {
	re := require.New(t)

	cmd := NewRegionWithKeyspaceCommand()
	cmd.PersistentFlags().String("pd", "http://mock-pd:2379", "")
	cmd.SetArgs([]string{"id", "1", "table", "100"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)

	re.NoError(cmd.Execute())
	re.Contains(out.String(), "the second argument should be table-id")
}

func expectedTableRangeInKeyspace(keyspaceID uint32, tableID int64) ([]byte, []byte) {
	keyspaceIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyspaceIDBytes, keyspaceID)

	keyPrefix := append([]byte{'x'}, keyspaceIDBytes[1:]...)
	startKey := codec.EncodeBytes(nil, append(keyPrefix, append([]byte{'t'}, codec.EncodeInt(nil, tableID)...)...))
	endKey := codec.EncodeBytes(nil, append(keyPrefix, append([]byte{'t'}, codec.EncodeInt(nil, tableID+1)...)...))
	return startKey, endKey
}
