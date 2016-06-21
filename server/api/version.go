package api

type version struct {
	Version string `json:"version"`
}

type versionController struct {
	baseController
}

func (c *versionController) Version() {
	c.Data["json"] = &version{
		Version: "1.0.0",
	}

	c.ServeJSON()
}
