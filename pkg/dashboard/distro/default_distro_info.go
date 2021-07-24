// +build !dashboard_distro

package distro

// Resource declared the distro brand information
var Resource = map[string]string{
	"tidb":    "TiDB",
	"tikv":    "TiKV",
	"tiflash": "TiFlash",
	"pd":      "PD",
}
