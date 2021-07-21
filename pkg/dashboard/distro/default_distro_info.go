// +build !distro

package distro

// Resource declared the distro brand information
var Resource = map[string]interface{}{
	"tidb":    "TiDB",
	"tikv":    "TiKV",
	"tiflash": "TiFlash",
	"pd":      "PD",
}
