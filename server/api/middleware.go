// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/golang/protobuf/jsonpb"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/cluster"
	"github.com/pingcap/pd/server/config"
	"github.com/pkg/errors"
	"github.com/unrolled/render"
)

type clusterMiddleware struct {
	s  *server.Server
	rd *render.Render
}

func newClusterMiddleware(s *server.Server) clusterMiddleware {
	return clusterMiddleware{
		s:  s,
		rd: render.New(render.Options{IndentJSON: true}),
	}
}

func (m clusterMiddleware) Middleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := m.s.GetRaftCluster()
		if rc == nil {
			m.rd.JSON(w, http.StatusInternalServerError, cluster.ErrNotBootstrapped.Error())
			return
		}
		ctx := withClusterCtx(r.Context(), rc)
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

type configMiddleware struct {
	s  *server.Server
	rd *render.Render
}

func newConfigMiddleware(s *server.Server) configMiddleware {
	return configMiddleware{
		s:  s,
		rd: render.New(render.Options{IndentJSON: true}),
	}
}

type entry struct {
	key   string
	value string
}

func (m configMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := make(map[string]interface{})
		json.NewDecoder(r.Body).Decode(&req)
		mapKeys := reflect.ValueOf(req).MapKeys()
		var entries []*entry
		for _, k := range mapKeys {
			if config.IsDeprecated(k.String()) {
				m.rd.JSON(w, http.StatusInternalServerError, errors.New("config item has already been deprecated").Error())
				return
			}
			itemMap := make(map[string]interface{})
			itemMap[k.String()] = req[k.String()]
			var buf bytes.Buffer
			if err := toml.NewEncoder(&buf).Encode(itemMap); err != nil {
				m.rd.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
			value := buf.String()
			key := findTag(reflect.TypeOf(&config.Config{}).Elem(), k.String())
			if key == "" {
				m.rd.JSON(w, http.StatusInternalServerError, errors.New("config item not found").Error())
				return
			}
			entries = append(entries, &entry{key, value})
		}

		s, err := newBody(m.s, entries)
		if err != nil {
			m.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		r.Body = ioutil.NopCloser(strings.NewReader(s))
		next.ServeHTTP(w, r)
	})
}

type adminMiddleware struct {
	s  *server.Server
	rd *render.Render
}

func newAdminMiddleware(s *server.Server) adminMiddleware {
	return adminMiddleware{
		s:  s,
		rd: render.New(render.Options{IndentJSON: true}),
	}
}

func (m adminMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req string
		json.NewDecoder(r.Body).Decode(&req)
		entries := []*entry{{key: "log.level", value: fmt.Sprintf("level = \"%v\"", req)}}
		s, err := newBody(m.s, entries)
		if err != nil {
			m.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		r.Body = ioutil.NopCloser(strings.NewReader(s))
		next.ServeHTTP(w, r)
	})
}

func newBody(s *server.Server, entries []*entry) (string, error) {
	clusterID := s.ClusterID()
	var configEntries []*configpb.ConfigEntry
	for _, e := range entries {
		configEntry := &configpb.ConfigEntry{Name: e.key, Value: e.value}
		configEntries = append(configEntries, configEntry)
	}
	version := s.GetConfigManager().GlobalCfgs[server.Component].GetVersion()

	req := &configpb.UpdateRequest{
		Header: &configpb.Header{
			ClusterId: clusterID,
		},
		Version: &configpb.Version{Global: version},
		Kind: &configpb.ConfigKind{
			Kind: &configpb.ConfigKind_Global{Global: &configpb.Global{Component: server.Component}},
		},
		Entries: configEntries,
	}

	m := jsonpb.Marshaler{}
	return m.MarshalToString(req)
}

func findTag(t reflect.Type, tag string) string {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		column := field.Tag.Get("json")
		c := strings.Split(column, ",")
		if c[0] == tag {
			return c[0]
		}

		if field.Type.Kind() == reflect.Struct {
			path := findTag(field.Type, tag)
			if path == "" {
				continue
			}
			return field.Tag.Get("json") + "." + path
		}
	}
	return ""
}
