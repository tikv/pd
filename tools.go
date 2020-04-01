// +build tools

package tools

import (
	_ "github.com/go-playground/overalls"
	_ "github.com/kevinburke/go-bindata/go-bindata"
	_ "github.com/mgechev/revive"
	_ "github.com/pingcap/failpoint/failpoint-ctl"
	_ "github.com/swaggo/swag/cmd/swag"
	_ "golang.org/x/tools/cmd/goimports"
)
