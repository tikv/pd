package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/cluster"
)

// GetRegions returns the regions.
func GetRegions() gin.HandlerFunc {
	return func(c *gin.Context) {
		rc := c.MustGet("cluster").(*cluster.RaftCluster)
		regions := rc.GetRegions()
		RegionsInfo := &RegionsInfo{
			Regions: make([]*RegionInfo, 0, len(regions)),
		}
		for _, r := range regions {
			RegionsInfo.Regions = append(RegionsInfo.Regions, newRegionInfo(r))
		}
		RegionsInfo.Count = len(RegionsInfo.Regions)
		c.IndentedJSON(http.StatusOK, RegionsInfo)
	}
}

// GetRegionByID returns the region according to the given ID.
func GetRegionByID() gin.HandlerFunc {
	return func(c *gin.Context) {
		rc := c.MustGet("cluster").(*cluster.RaftCluster)
		idParam := c.Param("id")
		id, err := strconv.ParseUint(idParam, 10, 64)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause().Error())
			return
		}
		region := rc.GetRegion(id)
		if region == nil {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}

		c.IndentedJSON(http.StatusOK, newRegionInfo(region))
	}
}
