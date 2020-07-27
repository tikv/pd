// Copyright 2020 PingCAP, Inc.
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

package autoscaling

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/server"
	"github.com/unrolled/render"
)

// Strategy within a HTTP request provides rules and resources to help make decision for auto scaling.
type Strategy struct {
	Rules                []*Rule                `json:"rules"`
	Resources            []*Resource            `json:"resources"`
	ResourceRequirements []*ResourceRequirement `json:"resource_requirements"`
}

// Rule is a set of constraints for a kind of component.
type Rule struct {
	Component               string       `json:"component"`
	CPURule                 *CPURule     `json:"cpu_rule,omitempty"`
	StorageRule             *StorageRule `json:"storage_rule,omitempty"`
	ScaleOutIntervalSeconds uint64       `json:"scale_out_interval_seconds"`
	ScaleInIntervalSeconds  uint64       `json:"scale_in_interval_seconds"`
}

// CPURule is the constraints about CPU.
type CPURule struct {
	MaxThreshold  float64  `json:"max_threshold"`
	MinThreshold  float64  `json:"min_threshold"`
	MaxCount      uint64   `json:"max_count"`
	ResourceTypes []string `json:"resource_types"`
}

// StorageRule is the constraints about storage.
type StorageRule struct {
	MinThreshold  float64  `json:"min_threshold"`
	MaxCount      uint64   `json:"max_count"`
	ResourceTypes []string `json:"resource_types"`
}

// Resource represents a kind of resource set including CPU, memory, storage.
type Resource struct {
	ResourceType string `json:"resource_type"`
	CPU          uint64 `json:"cpu"`
	Memory       uint64 `json:"memory"`
	Storage      uint64 `json:"storage"`
}

// Plan is the final result of auto scaling, which indicates how to scale in or scale out.
type Plan struct {
	Component    string               `json:"component"`
	Count        uint64               `json:"count"`
	ResourceType string               `json:"resource_type"`
	Labels       []*metapb.StoreLabel `json:"labels"`
}

// ResourceRequirement is a mechanism to control the resource.
type ResourceRequirement struct {
	Component   string `json:"component"`
	Requirement uint64 `json:"requirement"`
	Count       uint64 `json:"count"`
}

// HTTPHandler is a handler to handle the auto scaling HTTP request.
type HTTPHandler struct {
	svr *server.Server
	rd  *render.Render
}

// NewHTTPHandler creates a HTTPHandler.
func NewHTTPHandler(svr *server.Server, rd *render.Render) *HTTPHandler {
	return &HTTPHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	strategy := Strategy{}
	if err := json.Unmarshal(data, &strategy); err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	plan := calculate(&strategy)
	h.rd.JSON(w, http.StatusOK, plan)
}

// TODO: migrate the basic logic from operator.
func calculate(strategy *Strategy) *Plan {
	return &Plan{}
}
