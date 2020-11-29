package api

import (
	"bytes"
	"encoding/hex"
	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/schedule/anti"
	"github.com/unrolled/render"
	"net/http"
)

type antiRuleHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newAntiRulesHandler(svr *server.Server, rd *render.Render) *antiRuleHandler {
	return &antiRuleHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h antiRuleHandler) Set(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r.Context())
	var antiRule anti.AntiRule
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &antiRule); err != nil {
		return
	}
	if err := h.checkAntiRule(&antiRule); err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	cluster.GetAntiRuleManager().SetAntiRule(&antiRule)

	h.rd.JSON(w, http.StatusOK, "Set anti-rule successfully.")
}

func (h *antiRuleHandler) checkAntiRule(ar *anti.AntiRule) error {
	//TODO avoid rules setting with the same ID, more validation check should be considered
	start, err := hex.DecodeString(ar.StartKeyHex)
	if err != nil {
		return errors.Wrap(err, "start key is not in hex format")
	}
	end, err := hex.DecodeString(ar.EndKeyHex)
	if err != nil {
		return errors.Wrap(err, "end key is not hex format")
	}
	if len(end) > 0 && bytes.Compare(end, start) <= 0 {
		return errors.New("endKey should be greater than startKey")
	}
	return nil
}
