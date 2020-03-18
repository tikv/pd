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
	"net/url"
	"reflect"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gorilla/mux"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/cluster"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pkg/errors"
	"github.com/unrolled/render"
)

const (
	localKind  = "local"
	globalKind = "global"
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

type entry struct {
	key   string
	value string
}

type componentMiddleware struct {
	s  *server.Server
	rd *render.Render
}

func newComponentMiddleware(s *server.Server) componentMiddleware {
	return componentMiddleware{
		s:  s,
		rd: render.New(render.Options{IndentJSON: true}),
	}
}

func (m componentMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var statusCode int
		var err error
		switch r.Method {
		case "POST":
			r, statusCode, err = handleComponentPost(m.s, r)
			if err != nil {
				m.rd.JSON(w, statusCode, err.Error())
				return
			}
		case "GET":
			r, statusCode, err = handleComponentGet(m.s, r)
			if err != nil {
				m.rd.JSON(w, statusCode, err.Error())
				return
			}
		case "DELETE":
			r, statusCode, err = handleComponentDelete(m.s, r)
			if err != nil {
				m.rd.JSON(w, statusCode, err.Error())
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func getComponentInfo(req map[string]interface{}) string {
	var componentInfo string
	if c, ok := req["componentInfo"]; ok {
		componentInfo = c.(string)
	} else {
		componentInfo = ""
	}
	delete(req, "componentInfo")
	return componentInfo
}

func transToEntries(req map[string]interface{}) ([]*entry, error) {
	mapKeys := reflect.ValueOf(req).MapKeys()
	var entries []*entry
	for _, k := range mapKeys {
		if config.IsDeprecated(k.String()) {
			return nil, errors.New("config item has already been deprecated")
		}
		itemMap := make(map[string]interface{})
		itemMap[k.String()] = req[k.String()]
		var buf bytes.Buffer
		if err := toml.NewEncoder(&buf).Encode(itemMap); err != nil {
			return nil, err
		}
		value := buf.String()
		key := findTag(reflect.TypeOf(&config.Config{}).Elem(), k.String())
		if key == "" {
			return nil, errors.New("config item not found")
		}
		entries = append(entries, &entry{key, value})
	}
	return entries, nil
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

func updateBody(s *server.Server, component, componentID string, kind string, entries []*entry) (string, error) {
	clusterID := s.ClusterID()
	var configEntries []*configpb.ConfigEntry
	for _, e := range entries {
		configEntry := &configpb.ConfigEntry{Name: e.key, Value: e.value}
		configEntries = append(configEntries, configEntry)
	}
	var version *configpb.Version
	var k *configpb.ConfigKind
	cm := s.GetConfigManager()
	cm.RLock()
	defer cm.RUnlock()
	switch kind {
	case localKind:
		version = cm.LocalCfgs[component][componentID].GetVersion()
		k = &configpb.ConfigKind{Kind: &configpb.ConfigKind_Local{Local: &configpb.Local{ComponentId: componentID}}}
	case globalKind:
		version = &configpb.Version{Global: cm.GlobalCfgs[component].GetVersion()}
		k = &configpb.ConfigKind{Kind: &configpb.ConfigKind_Global{Global: &configpb.Global{Component: component}}}
	default:
		return "", errors.New("no valid kind")
	}

	req := &configpb.UpdateRequest{
		Header: &configpb.Header{
			ClusterId: clusterID,
		},
		Version: version,
		Kind:    k,
		Entries: configEntries,
	}

	m := jsonpb.Marshaler{}
	return m.MarshalToString(req)
}

func handleComponentPost(s *server.Server, r *http.Request) (*http.Request, int, error) {
	var component, componentID, kind string
	req := make(map[string]interface{})
	json.NewDecoder(r.Body).Decode(&req)
	cm := s.GetConfigManager()
	componentInfo := getComponentInfo(req)
	cm.RLock()
	component = cm.GetComponent(componentInfo)
	cm.RUnlock()
	if component == "" {
		component = componentInfo
		kind = globalKind
	} else {
		componentID = componentInfo
		kind = localKind
	}
	mapKeys := reflect.ValueOf(req).MapKeys()
	var entries []*entry
	for _, k := range mapKeys {
		var value string
		switch req[k.String()].(type) {
		case float64, float32:
			value = fmt.Sprintf("%f", req[k.String()])
		default:
			value = fmt.Sprintf("%v", req[k.String()])
		}
		entries = append(entries, &entry{k.String(), value})
	}
	str, err := updateBody(s, component, componentID, kind, entries)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	u, err := url.ParseRequestURI("/component")
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	r.URL = u
	r.Body = ioutil.NopCloser(strings.NewReader(str))
	return r, http.StatusOK, nil
}

func handleComponentGet(s *server.Server, r *http.Request) (*http.Request, int, error) {
	var component string
	vars := mux.Vars(r)
	varName := "component_id"
	componentID, ok := vars[varName]
	if !ok {
		return nil, http.StatusBadRequest, errors.Errorf("field %s is not present", varName)
	}
	cm := s.GetConfigManager()
	cm.RLock()
	component = cm.GetComponent(componentID)
	version := cm.GetLatestVersion(component, componentID)
	cm.RUnlock()
	if component == "" {
		return nil, http.StatusBadRequest, errors.Errorf("cannot find component with component ID: %s", componentID)
	}
	clusterID := s.ClusterID()
	getURI := fmt.Sprintf("/component?header.cluster_id=%d&component=%s&component_id=%s&version.local=%d&version.global=%d",
		clusterID, component, componentID, version.GetLocal(), version.GetGlobal())
	u, err := url.ParseRequestURI(getURI)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	r.URL = u
	r.Header.Set("Accept", "application/toml")
	return r, http.StatusOK, nil
}

func handleComponentDelete(s *server.Server, r *http.Request) (*http.Request, int, error) {
	var component string
	vars := mux.Vars(r)
	varName := "component_id"
	componentID, ok := vars[varName]
	if !ok {
		return nil, http.StatusBadRequest, errors.Errorf("field %s is not present", varName)
	}
	cm := s.GetConfigManager()
	cm.RLock()
	component = cm.GetComponent(componentID)
	version := cm.GetLatestVersion(component, componentID)
	cm.RUnlock()
	clusterID := s.ClusterID()
	getURI := fmt.Sprintf("/component?header.cluster_id=%d&kind.local.component_id=%s&version.local=%d&version.global=%d",
		clusterID, componentID, version.GetLocal(), version.GetGlobal())
	u, err := url.ParseRequestURI(getURI)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	r.URL = u
	return r, http.StatusOK, nil
}
