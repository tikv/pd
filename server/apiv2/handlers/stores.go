// Copyright 2022 TiKV Project Authors.
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

package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/cluster"
)

// GetStores returns the stores.
func GetStores() gin.HandlerFunc {
	return func(c *gin.Context) {
		rc := c.MustGet("cluster").(*cluster.RaftCluster)
		stores := rc.GetMetaStores()
		StoresInfo := &StoresInfo{
			Stores: make([]*StoreInfo, 0, len(stores)),
		}

		for _, s := range stores {
			storeID := s.GetId()
			store := rc.GetStore(storeID)
			if store == nil {
				c.AbortWithStatusJSON(
					http.StatusInternalServerError,
					gin.H{
						"error": errs.ErrStoreNotFound.FastGenByArgs(storeID).Error(),
					},
				)
				return
			}

			storeInfo := newStoreInfo(rc.GetOpts().GetScheduleConfig(), store)
			StoresInfo.Stores = append(StoresInfo.Stores, storeInfo)
		}
		StoresInfo.Count = len(StoresInfo.Stores)
		c.IndentedJSON(
			http.StatusOK,
			gin.H{
				"stores": StoresInfo,
			},
		)
	}
}

// GetStoreByID returns the store according to the given ID.
// @Tags store
// @version 2.0
// @Summary Get a store's information.
// @Param id path integer true "Store Id"
// @Produce json
// @Success 200 {object} StoreInfo
// @Failure 400 {string} string "The input is invalid."
// @Failure 404 {string} string "The store does not exist."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /stores/{id} [get]
func GetStoreByID() gin.HandlerFunc {
	return func(c *gin.Context) {
		rc := c.MustGet("cluster").(*cluster.RaftCluster)
		idParam := c.Param("id")
		id, err := strconv.ParseUint(idParam, 10, 64)
		if err != nil {
			c.AbortWithStatusJSON(
				http.StatusBadRequest,
				gin.H{
					"error": errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause().Error(),
				},
			)
		}
		store := rc.GetStore(id)
		if store == nil {
			c.AbortWithStatusJSON(
				http.StatusInternalServerError,
				gin.H{
					"error": errs.ErrStoreNotFound.FastGenByArgs(id).Error(),
				},
			)
			return
		}

		storeInfo := newStoreInfo(rc.GetOpts().GetScheduleConfig(), store)
		c.IndentedJSON(
			http.StatusOK,
			gin.H{
				"store": storeInfo,
			},
		)
	}
}

// DeleteStoreByID will delete the store according to the given ID.
func DeleteStoreByID() gin.HandlerFunc {
	return func(c *gin.Context) {
		rc := c.MustGet("cluster").(*cluster.RaftCluster)
		idParam := c.Param("id")
		id, err := strconv.ParseUint(idParam, 10, 64)
		if err != nil {
			c.AbortWithStatusJSON(
				http.StatusBadRequest,
				gin.H{
					"error": errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause().Error(),
				},
			)
		}

		var force bool
		forceQuery := c.Query("force")
		if forceQuery != "" {
			force, err = strconv.ParseBool(forceQuery)
			if err != nil {
				c.IndentedJSON(
					http.StatusBadRequest,
					gin.H{
						"error": errs.ErrStrconvParseBool.Wrap(err).FastGenWithCause().Error(),
					},
				)
			}
		}

		err = rc.RemoveStore(id, force)
		if err != nil {
			c.AbortWithStatusJSON(
				http.StatusBadRequest,
				gin.H{
					"error": err.Error(),
				},
			)
			return
		}

		c.JSON(
			http.StatusOK,
			nil,
		)
	}
}
