package tso

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestAdjust(t *testing.T) {
	re := require.New(t)

	// TSO config testings
	cfgData := `
tso-update-physical-interval = "3m"
tso-save-interval = "1m"
enable-local-tso = true
`
	cfg := NewConfig()
	meta, err := toml.Decode(cfgData, &cfg)
	re.NoError(err)
	err = cfg.adjust(&meta)
	re.NoError(err)

	re.Equal(maxTSOUpdatePhysicalInterval, cfg.TSOUpdatePhysicalInterval.Duration)
	re.Equal(1*time.Minute, cfg.TSOSaveInterval.Duration)
	re.True(cfg.EnableLocalTSO)

	cfgData = `
tso-update-physical-interval = "1ns"
	`
	cfg = NewConfig()
	meta, err = toml.Decode(cfgData, &cfg)
	re.NoError(err)
	err = cfg.adjust(&meta)
	re.NoError(err)

	re.Equal(minTSOUpdatePhysicalInterval, cfg.TSOUpdatePhysicalInterval.Duration)
	re.Equal(defaultTSOSaveInterval, cfg.TSOSaveInterval.Duration)
	re.False(cfg.EnableLocalTSO)
}
