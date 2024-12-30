// Copyright 2017 TiKV Project Authors.
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
	"net/http"
	"regexp"
	"strings"

	"github.com/unrolled/render"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/response"
	"github.com/tikv/pd/server"
)

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

// @Tags     label
// @Summary  List all label values.
// @Produce  json
// @Success  200  {array}  metapb.StoreLabel
// @Router   /labels [get]
func (h *labelsHandler) GetLabels(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	var labels []*metapb.StoreLabel
	m := make(map[string]struct{})
	stores := rc.GetStores()
	for _, s := range stores {
		ls := s.GetLabels()
		for _, l := range ls {
			if _, ok := m[strings.ToLower(l.Key+l.Value)]; !ok {
				m[strings.ToLower(l.Key+l.Value)] = struct{}{}
				labels = append(labels, l)
			}
		}
	}
	h.rd.JSON(w, http.StatusOK, labels)
}

// @Tags     label
// @Summary  List stores that have specific label values.
// @Param    name   query  string  true  "name of store label filter"
// @Param    value  query  string  true  "value of store label filter"
// @Produce  json
// @Success  200  {object}  response.StoresInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /labels/stores [get]
func (h *labelsHandler) GetStoresByLabel(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	name := r.URL.Query().Get("name")
	value := r.URL.Query().Get("value")
	filter, err := newStoresLabelFilter(name, value)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	stores := rc.GetMetaStores()
	storesInfo := &response.StoresInfo{
		Stores: make([]*response.StoreInfo, 0, len(stores)),
	}

	stores = filter.filter(stores)
	for _, s := range stores {
		storeID := s.GetId()
		store := rc.GetStore(storeID)
		if store == nil {
			h.rd.JSON(w, http.StatusInternalServerError, errs.ErrStoreNotFound.FastGenByArgs(storeID).Error())
			return
		}

		storeInfo := response.BuildStoreInfo(h.svr.GetScheduleConfig(), store)
		storesInfo.Stores = append(storesInfo.Stores, storeInfo)
	}
	storesInfo.Count = len(storesInfo.Stores)

	h.rd.JSON(w, http.StatusOK, storesInfo)
}

type storesLabelFilter struct {
	keyPattern   *regexp.Regexp
	valuePattern *regexp.Regexp
}

func newStoresLabelFilter(name, value string) (*storesLabelFilter, error) {
	// add (?i) to set a case-insensitive flag
	keyPattern, err := regexp.Compile("(?i)" + name)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	valuePattern, err := regexp.Compile("(?i)" + value)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &storesLabelFilter{
		keyPattern:   keyPattern,
		valuePattern: valuePattern,
	}, nil
}

func (filter *storesLabelFilter) filter(stores []*metapb.Store) []*metapb.Store {
	ret := make([]*metapb.Store, 0, len(stores))
	for _, s := range stores {
		ls := s.GetLabels()
		for _, l := range ls {
			isKeyMatch := filter.keyPattern.MatchString(l.Key)
			isValueMatch := filter.valuePattern.MatchString(l.Value)
			if isKeyMatch && isValueMatch {
				ret = append(ret, s)
				break
			}
		}
	}
	return ret
}
