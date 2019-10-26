package nodeutil

import "testing"

func TestGetStaticStatsInfo(t *testing.T) {
	t.Log(GetStaticStatsInfo())
}

func TestGetStatsInfo(t *testing.T) {
	t.Log(GetStatsInfo())
}

func TestGetNetworkLatency(t *testing.T) {
	t.Log(GetNetworkLatency())
}
