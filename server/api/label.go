package api

type labelHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newLabelsHandler(svr *server.Server, rd *render.Render) *labelHandler {
	return &labelHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *labelHandler) List(w http.ResponseWriter, r *http.Requst) {

}
