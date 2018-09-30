package simulator

import (
	"time"

	"github.com/pingcap/pd/pkg/typeutil"
)

const (
	defaultSimTickInterval    = 100 * time.Millisecond
	defaultNormTickInterval   = 1 * time.Second
	defaultStoreCapacityGB    = 1024
	defaultStoreAvailableGB   = 1024
	defaultStoreIOMBPerSecond = 40
	defaultStoreVersion       = "2.1.0"
)

// SimConfig is the simulator configuration.
type SimConfig struct {
	SimTickInterval  typeutil.Duration `toml:"sim-tick-interval"`
	NormTickInterval typeutil.Duration `toml:"norm-tick-interval"`

	StoreCapacityGB    uint64 `toml:"store-capacity"`
	StoreAvailableGB   uint64 `toml:"store-available"`
	StoreIOMBPerSecond int64  `toml:"store-io-per-second"`
	StoreVersion       string `toml:"store-version"`
}

// NewSimConfig create a new configuration of the simulator.
func NewSimConfig() *SimConfig {
	return &SimConfig{}
}

func adjustDuration(v *typeutil.Duration, defValue time.Duration) {
	if v.Duration == 0 {
		v.Duration = defValue
	}
}

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

func adjustUint64(v *uint64, defValue uint64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustInt64(v *int64, defValue int64) {
	if *v == 0 {
		*v = defValue
	}
}

// Adjust is used to adjust configurations
func (sc *SimConfig) Adjust() {
	adjustDuration(&sc.SimTickInterval, defaultSimTickInterval)
	adjustDuration(&sc.NormTickInterval, defaultNormTickInterval)
	adjustUint64(&sc.StoreCapacityGB, defaultStoreCapacityGB)
	adjustUint64(&sc.StoreAvailableGB, defaultStoreAvailableGB)
	adjustInt64(&sc.StoreIOMBPerSecond, defaultStoreIOMBPerSecond)
	adjustString(&sc.StoreVersion, defaultStoreVersion)
}
