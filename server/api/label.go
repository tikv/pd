// Copyright 2017 PingCAP, Inc.
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
	"net/http"
	"regexp"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

var (
	errLabelConflictFilterRequst = errors.New("Cannot use string and regexp in same time")
	errLabelRegex                = errors.New("Cannot compile regexp, Please check it")
)

type labelInfo struct {
	ID      uint64               `json:"store_id"`
	Address string               `json:"address"`
	Labels  []*metapb.StoreLabel `json:"labels"`
}

func newLabelInfo(store *metapb.Store) *labelInfo {
	return &labelInfo{
		ID:      store.Id,
		Address: store.Address,
		Labels:  store.Labels,
	}
}

type labelsHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newLabelsHandler(svr *server.Server, rd *render.Render) *labelsHandler {
	return &labelsHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *labelsHandler) Get(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, errNotBootstrapped.Error())
		return
	}
	var labels []*metapb.StoreLabel
	m := make(map[string]bool)
	stores := cluster.GetStores()
	for _, s := range stores {
		ls := s.GetLabels()
		for _, l := range ls {
			if ok := m[l.Key+l.Value]; !ok {
				m[l.Key+l.Value] = true
				labels = append(labels, l)
			}
		}
	}
	h.rd.JSON(w, http.StatusOK, labels)
}

func (h *labelsHandler) List(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, errNotBootstrapped.Error())
		return
	}
	var labels []*labelInfo
	stores := cluster.GetStores()
	for _, s := range stores {
		label := newLabelInfo(s)
		labels = append(labels, label)
	}
	h.rd.JSON(w, http.StatusOK, labels)
}

func (h *labelsHandler) GetStore(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, errNotBootstrapped.Error())
		return
	}

	name := r.URL.Query().Get("name")
	value := r.URL.Query().Get("value")
	nameReg := r.URL.Query().Get("name_re")
	valueReg := r.URL.Query().Get("value_re")
	filter, err := newStoresLabelFilter(name, nameReg, value, valueReg)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	stores := cluster.GetStores()
	stores = filter.filter(stores)
	h.rd.JSON(w, http.StatusOK, stores)
}

type storesLabelFilter struct {
	nameFilter  *storeLabelFilter
	valueFilter *storeLabelFilter
}

func newStoresLabelFilter(name, nameReg, value, valueReg string) (*storesLabelFilter, error) {
	nameFilter, err := newStoreLabelStringFilter(name, nameReg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	valueFilter, err := newStoreLabelStringFilter(value, valueReg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &storesLabelFilter{
		nameFilter:  nameFilter,
		valueFilter: valueFilter,
	}, nil
}

func (filter *storesLabelFilter) filter(stores []*metapb.Store) []*metapb.Store {
	ret := make([]*metapb.Store, 0, len(stores))
	for _, s := range stores {
		ls := s.GetLabels()
		for _, l := range ls {
			isKeyMatch := filter.nameFilter.filterLabelKey(l)
			isValueMatch := filter.valueFilter.filterLabelValue(l)
			if isKeyMatch && isValueMatch {
				ret = append(ret, s)
				break
			}
		}

	}
	return ret
}

type storeLabelFilter struct {
	pattern    string
	patternReg *regexp.Regexp
	isReg      bool
}

func newStoreLabelStringFilter(pattern, patternRegString string) (*storeLabelFilter, error) {
	var (
		isReg      bool
		patternReg *regexp.Regexp
		err        error
	)

	if pattern != "" && patternRegString != "" {
		return nil, errLabelConflictFilterRequst
	}
	if pattern == "" {
		isReg = true
		patternReg, err = regexp.Compile(patternRegString)
		if err != nil {
			return nil, errLabelRegex
		}
	}
	return &storeLabelFilter{
		pattern:    pattern,
		isReg:      isReg,
		patternReg: patternReg,
	}, nil
}

func (filter *storeLabelFilter) filterLabelKey(l *metapb.StoreLabel) bool {
	if filter.isReg {
		if filter.patternReg.MatchString(l.Key) {
			return true
		}
	}
	return l.Key == filter.pattern
}

func (filter *storeLabelFilter) filterLabelValue(l *metapb.StoreLabel) bool {
	if filter.isReg {
		if filter.patternReg.MatchString(l.Value) {
			return true
		}
	}
	return l.Value == filter.pattern
}
