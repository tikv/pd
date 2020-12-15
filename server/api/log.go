// Copyright 2018 TiKV Project Authors.
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
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
	"go.uber.org/zap/zapcore"
)

type logHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newLogHandler(svr *server.Server, rd *render.Render) *logHandler {
	return &logHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags admin
// @Summary Set log level.
// @Accept json
// @Param level body string true "json params"
// @Produce json
// @Success 200 {string} string "The log level is updated."
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Failure 503 {string} string "PD server has no leader."
// @Router /admin/log [post]
func (h *logHandler) SetLogLevel(w http.ResponseWriter, r *http.Request) {
	var level string
	data, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	err = json.Unmarshal(data, &level)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	err = h.svr.SetLogLevel(level)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	log.SetLevel(logutil.StringToZapLogLevel(level))

	h.rd.JSON(w, http.StatusOK, "The log level is updated.")
}

const defaultGetLogSecond = 600 // 10 minutes

// @Tags admin
// @Summary Get logs.
// @Param name query []string true "name" collectionFormat(multi)
// @Param second query integer false "duration of getting"
// @Param format query string false "log format" Enums(text, json)
// @Param level query string false "log level" Enums(debug, info, warn, error, panic, fatal)
// @Produce plain
// @Success 200 {string} string "Finished getting logs."
// @Failure 400 {string} string "The input is invalid."
// @Router /admin/log [get]
func (h *logHandler) GetLog(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	// get second
	var second int64
	if secondStr := query.Get("second"); secondStr != "" {
		var err error
		second, err = strconv.ParseInt(secondStr, 10, 64)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		if second <= 0 {
			h.rd.JSON(w, http.StatusBadRequest, "duration is less than 0.")
			return
		}
	}
	if second == 0 {
		second = defaultGetLogSecond
	}

	// get names
	names := query["name"]
	if len(names) == 0 {
		h.rd.JSON(w, http.StatusBadRequest, "empty name.")
		return
	}

	// get format and level
	logConfig := h.svr.GetConfig().Log
	logConfig.Format = "json" // default
	logConfig.Level = "debug" // default
	if format := query.Get("format"); format != "" {
		if format == "text" || format == "json" {
			logConfig.Format = format
		} else {
			h.rd.JSON(w, http.StatusBadRequest, "wrong log format.")
			return
		}
	}
	if level := query.Get("level"); level != "" {
		var logLevel zapcore.Level
		if err := logLevel.Set(level); err == nil {
			logConfig.Level = level
		} else {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	httpLogger, err := logutil.NewHTTPLogger(&logConfig, w)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	defer httpLogger.Close()

	if err := httpLogger.Plug(names...); err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	select {
	case <-time.After(time.Duration(second) * time.Second):
	case <-r.Context().Done():
	}
}
