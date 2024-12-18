package server

import (
	"encoding/json"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

func TestPatchResourceGroup(t *testing.T) {
	re := require.New(t)
	rg := &ResourceGroup{Name: "test", Mode: rmpb.GroupMode_RUMode, RUSettings: NewRequestUnitSettings(nil)}
	testCaseRU := []struct {
		patchJSONString  string
		expectJSONString string
	}{
		{`{"name":"test", "mode":1, "r_u_settings": {"r_u":{"settings":{"fill_rate": 200000}}}}`,
			`{"name":"test","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":200000},"state":{"initialized":false}}},"priority":0}`},
		{`{"name":"test", "mode":1, "r_u_settings": {"r_u":{"settings":{"fill_rate": 200000, "burst_limit": -1}}}}`,
			`{"name":"test","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":200000,"burst_limit":-1},"state":{"initialized":false}}},"priority":0}`},
		{`{"name":"test", "mode":1, "r_u_settings": {"r_u":{"settings":{"fill_rate": 200000, "burst_limit": -1}}}, "priority": 8, "runaway_settings": {"rule":{"exec_elapsed_time_ms":10000}, "action":1} }`,
			`{"name":"test","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":200000,"burst_limit":-1},"state":{"initialized":false}}},"priority":8,"runaway_settings":{"rule":{"exec_elapsed_time_ms":10000},"action":1}}`},
	}

	for _, ca := range testCaseRU {
		patch := &rmpb.ResourceGroup{}
		err := json.Unmarshal([]byte(ca.patchJSONString), patch)
		re.NoError(err)
		err = rg.PatchSettings(patch)
		re.NoError(err)
		res, err := json.Marshal(rg.Clone(false))
		re.NoError(err)
		re.Equal(ca.expectJSONString, string(res))
	}
}

func TestClone(t *testing.T) {
	for i := 0; i <= 10; i++ {
		var rg ResourceGroup
		gofakeit.Struct(&rg)
		rgClone := rg.Clone(true)
		require.EqualValues(t, &rg, rgClone)
	}
}
