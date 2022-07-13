package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/keyspace"
)

func RegisterKeyspace(r *gin.RouterGroup) {
	router := r.Group("keyspaces")
	router.POST("", CreateKeyspace)
	router.GET("", LoadAllKeyspaces)
	router.GET("/:name", LoadKeyspace)
	router.PATCH("/:name", UpdateKeyspace)
}

type CreateKeyspaceParams struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
}

func CreateKeyspace(c *gin.Context) {
	svr := c.MustGet("server").(*server.Server)
	manager := svr.GetKeyspaceManager()
	createParams := &CreateKeyspaceParams{}
	err := c.BindJSON(createParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	req := &keyspace.CreateKeyspaceRequest{
		Name:          createParams.Name,
		InitialConfig: createParams.Config,
		Now:           time.Now(),
	}
	meta, err := manager.CreateKeyspace(req)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, &KeyspaceMeta{meta})
}

func LoadKeyspace(c *gin.Context) {
	svr := c.MustGet("server").(*server.Server)
	manager := svr.GetKeyspaceManager()
	name := c.Param("name")
	meta, err := manager.LoadKeyspace(name)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, &KeyspaceMeta{meta})
}

// parseLoadAllQuery parses LoadAllKeyspaces' query parameters.
// page_token:
// The keyspace id of the scan start. If not set, scan from keyspace with id 1.
// It's string of spaceID of the previous scan result's last element (next_page_token).
// limit:
// The maximum number of keyspace metas to return. If not set, no limit is posed.
// Every scan scans limit + 1 keyspaces (if limit != 0), the extra scanned keyspace
// is to check if there's more, and used to set next_page_token in response.
func parseLoadAllQuery(c *gin.Context) (scanStart uint32, scanLimit int, err error) {
	pageToken, set := c.GetQuery("page_token")
	if !set || pageToken == "" {
		// If pageToken is empty or unset, then scan from spaceID of 1.
		scanStart = 1
	} else {
		scanStart64, err := strconv.ParseUint(pageToken, 10, 32)
		if err != nil {
			return 0, 0, err
		}
		scanStart = uint32(scanStart64)
	}

	limitStr, set := c.GetQuery("limit")
	if !set || limitStr == "" || limitStr == "0" {
		// If limit is unset or empty or 0, then no limit is posed for scan.
		scanLimit = 0
	} else {
		scanLimit64, err := strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			return 0, 0, err
		}
		// Scan an extra element for next_page_token.
		scanLimit = int(scanLimit64) + 1
	}

	return scanStart, scanLimit, nil
}

type LoadAllKeyspacesResponse struct {
	Keyspaces []*KeyspaceMeta `json:"keyspaces"`
	// Token that can be used to read immediate next page.
	// If it's empty, then end has been reached.
	NextPageToken string `json:"next_page_token"`
}

func LoadAllKeyspaces(c *gin.Context) {
	svr := c.MustGet("server").(*server.Server)
	manager := svr.GetKeyspaceManager()
	scanStart, scanLimit, err := parseLoadAllQuery(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
		return
	}
	scanned, err := manager.LoadRangeKeyspace(scanStart, scanLimit)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	resp := &LoadAllKeyspacesResponse{}
	// If scanned 0 keyspaces, return result immediately.
	if len(scanned) == 0 {
		c.IndentedJSON(http.StatusOK, resp)
		return
	}
	var resultKeyspaces []*KeyspaceMeta
	if scanLimit == 0 || len(scanned) < scanLimit {
		// No next page, all scanned are results.
		resultKeyspaces = make([]*KeyspaceMeta, len(scanned))
		for i, meta := range scanned {
			resultKeyspaces[i] = &KeyspaceMeta{meta}
		}
	} else {
		// Scanned limit + 1 keyspaces, there is next page, all but last are results.
		resultKeyspaces = make([]*KeyspaceMeta, len(scanned)-1)
		for i := range resultKeyspaces {
			resultKeyspaces[i] = &KeyspaceMeta{scanned[i]}
		}
		// Also set next_page_token here.
		resp.NextPageToken = strconv.Itoa(int(scanned[len(scanned)-1].Id))
	}
	resp.Keyspaces = resultKeyspaces
	c.IndentedJSON(http.StatusOK, resp)
}

type UpdateKeyspaceParams struct {
	State string `json:"state"`
	// Note: Config's values are string pointers.
	// This is to differentiate between empty string "" and null value during binding.
	// This is especially important when applying JSON merge patch, where null value means to remove,
	// whereas empty string should simply set.
	Config map[string]*string `json:"config"`
}

func UpdateKeyspace(c *gin.Context) {
	svr := c.MustGet("server").(*server.Server)
	manager := svr.GetKeyspaceManager()
	name := c.Param("name")
	updateParams := &UpdateKeyspaceParams{}
	err := c.BindJSON(updateParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	req, err := getUpdateRequest(name, updateParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
		return
	}
	meta, err := manager.UpdateKeyspace(req)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, &KeyspaceMeta{meta})
}

// getUpdateRequest converts updateKeyspaceParams to keyspace.UpdateKeyspaceRequest.
func getUpdateRequest(name string, params *UpdateKeyspaceParams) (*keyspace.UpdateKeyspaceRequest, error) {
	req := &keyspace.UpdateKeyspaceRequest{
		Name: name,
	}
	if params.State == "" {
		req.UpdateState = false
	} else {
		req.UpdateState = true
		req.Now = time.Now()
		stateVal, ok := keyspacepb.KeyspaceState_value[params.State]
		if !ok {
			return nil, errors.New("Illegal keyspace state")
		}
		req.NewState = keyspacepb.KeyspaceState(stateVal)
	}
	toPut := map[string]string{}
	var toDelete []string
	for k, v := range params.Config {
		if v == nil {
			toDelete = append(toDelete, k)
		} else {
			toPut[k] = *v
		}
	}
	if len(toPut) > 0 {
		req.ToPut = toPut
	}
	if len(toDelete) > 0 {
		req.ToDelete = toDelete
	}
	return req, nil
}

// KeyspaceMeta wraps keyspacepb.KeyspaceMeta to provide custom JSON marshal.
type KeyspaceMeta struct {
	*keyspacepb.KeyspaceMeta
}

// MarshalJSON creates custom marshal of KeyspaceMeta with the following:
// 1. Keyspace ID are removed from marshal result to avoid exposure of internal mechanics.
// 2. Keyspace State are marshaled to their corresponding name for better readability.
func (meta *KeyspaceMeta) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Name           string            `json:"name,omitempty"`
		State          string            `json:"state,omitempty"`
		CreatedAt      int64             `json:"created_at,omitempty"`
		StateChangedAt int64             `json:"state_changed_at,omitempty"`
		Config         map[string]string `json:"config,omitempty"`
	}{
		meta.Name,
		keyspacepb.KeyspaceState_name[int32(meta.State)],
		meta.CreatedAt,
		meta.StateChangedAt,
		meta.Config,
	})
}
