package server

import (
	"encoding/json"
	"testing"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/require"
)

func TestPatchResourceGroup(t *testing.T) {
	re := require.New(t)
	rg1 := &ResourceGroup{Name: "test", Mode: rmpb.GroupMode_RUMode, RUSettings: &RequestUnitSettings{}}
	err := rg1.CheckAndInit()
	re.NoError(err)
	testCaseRU := []struct {
		patchJSONString  string
		expectJSONString string
	}{
		{`{"name":"test", "mode":1, "r_u_settings": {"r_u":{"settings":{"fill_rate": 200000}}}}`,
			`{"name":"test","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":200000},"state":{"initialized":false}}}}`},
		{`{"name":"test", "mode":1, "r_u_settings": {"r_u":{"settings":{"fill_rate": 200000, "burst_limit": -1}}}}`,
			`{"name":"test","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":200000,"burst_limit":-1},"state":{"initialized":false}}}}`},
	}

	for _, ca := range testCaseRU {
		rg := rg1.Copy()
		patch := &rmpb.ResourceGroup{}
		err := json.Unmarshal([]byte(ca.patchJSONString), patch)
		re.NoError(err)
		err = rg.PatchSettings(patch)
		re.NoError(err)
		res, err := json.Marshal(rg)
		re.NoError(err)
		re.Equal(ca.expectJSONString, string(res))
	}

	rg2 := &ResourceGroup{Name: "test", Mode: rmpb.GroupMode_RawMode, RawResourceSettings: &RawResourceSettings{}}
	err = rg2.CheckAndInit()
	re.NoError(err)
	testCaseResource := []struct {
		patchJSONString  string
		expectJSONString string
	}{
		{`{"name":"test", "mode":2, "raw_resource_settings": {"cpu":{"settings":{"fill_rate": 200000}}}}`,
			`{"name":"test","mode":2,"raw_resource_settings":{"cpu":{"settings":{"fill_rate":200000},"state":{"initialized":false}},"io_read_bandwidth":{"state":{"initialized":false}},"io_write_bandwidth":{"state":{"initialized":false}}}}`},
		{`{"name":"test", "mode":2, "raw_resource_settings": {"io_read":{"settings":{"fill_rate": 200000,"burst_limit":1000000}}}}`,
			`{"name":"test","mode":2,"raw_resource_settings":{"cpu":{"state":{"initialized":false}},"io_read_bandwidth":{"settings":{"fill_rate":200000,"burst_limit":1000000},"state":{"initialized":false}},"io_write_bandwidth":{"state":{"initialized":false}}}}`},
		{`{"name":"test", "mode":2, "raw_resource_settings": {"io_write":{"settings":{"fill_rate": 200000}}}}`,
			`{"name":"test","mode":2,"raw_resource_settings":{"cpu":{"state":{"initialized":false}},"io_read_bandwidth":{"state":{"initialized":false}},"io_write_bandwidth":{"settings":{"fill_rate":200000},"state":{"initialized":false}}}}`},
	}

	for _, ca := range testCaseResource {
		rg := rg2.Copy()
		patch := &rmpb.ResourceGroup{}
		err := json.Unmarshal([]byte(ca.patchJSONString), patch)
		re.NoError(err)
		err = rg.PatchSettings(patch)
		re.NoError(err)
		res, err := json.Marshal(rg)
		re.NoError(err)
		re.Equal(ca.expectJSONString, string(res))
	}
}
