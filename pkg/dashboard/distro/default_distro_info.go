// +build !distro

package distro

var Resource = map[string]interface{}{
	"tidb":    "TiDB",
	"tikv":    "TiKV",
	"tiflash": "TiFlash",
	"pd":      "PD",
}
