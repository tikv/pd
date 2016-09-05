// Copyright 2016 PingCAP, Inc.
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
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type feedHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newFeedHandler(svr *server.Server, rd *render.Render) *feedHandler {
	return &feedHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *feedHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, errNotBootstrapped.Error())
		return
	}

	offsetStr := r.URL.Query().Get("offset")
	if len(offsetStr) == 0 {
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}

	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	evts := cluster.FetchEvents(offset, false)
	h.rd.JSON(w, http.StatusOK, evts)
}

type eventsHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newEventsHandler(svr *server.Server, rd *render.Render) *eventsHandler {
	return &eventsHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *eventsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, errNotBootstrapped.Error())
		return
	}

	evts := cluster.FetchEvents(0, true)
	h.rd.JSON(w, http.StatusOK, evts)
}

type wsHandler struct {
	sync.RWMutex

	upgrader websocket.Upgrader
	chs      map[*http.Request]chan server.LogEvent
	evtCh    chan server.LogEvent

	offset uint64
	svr    *server.Server
}

func newWSHandler(svr *server.Server) *wsHandler {
	h := &wsHandler{
		chs:   make(map[*http.Request]chan server.LogEvent, 1000),
		evtCh: make(chan server.LogEvent, 100),
		svr:   svr,
	}

	go h.fanout()
	go h.fetchEventFeed()

	return h
}

func (h *wsHandler) fanout() {
	for evt := range h.evtCh {
		h.RLock()
		for _, ch := range h.chs {
			select {
			case ch <- evt:
			default:
			}
		}
		h.RUnlock()
	}
}

func (h *wsHandler) fetchEventFeed() {
	for {
		time.Sleep(1 * time.Second)

		cluster := h.svr.GetRaftCluster()
		if cluster == nil {
			continue
		}

		evts := cluster.FetchEvents(h.offset, false)

		for _, evt := range evts {
			h.evtCh <- evt
			h.offset = evt.ID
		}
	}
}

func (h *wsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()

	// Make sure the client is alive
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	ch := make(chan server.LogEvent, 100)
	h.Lock()
	h.chs[r] = ch
	h.Unlock()

	defer func() {
		h.Lock()
		log.Info("client is closed, removing channel")
		close(h.chs[r])
		delete(h.chs, r)
		h.Unlock()
	}()

	for {
		select {
		case <-ticker.C:
			if err := c.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				log.Error(err)
				return
			}
		case event := <-ch:
			logMsg, err := json.Marshal(event)
			if err != nil {
				log.Error(err)
				return
			}

			err = c.WriteMessage(websocket.TextMessage, logMsg)
			if err != nil {
				log.Error(err)
				return
			}
		}
	}
}
