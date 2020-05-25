package uiserver

import (
	"html"
	"os"
	"strings"
)

type modifiedFileInfo struct {
	os.FileInfo
	size int64
}

func (f modifiedFileInfo) Size() int64 {
	return f.size
}

func (f modifiedFileInfo) Sys() interface{} {
	return nil
}

func InitAssetFS(prefix string) {
	rewrite := func(assetPath string) {
		a, err := _bindata[assetPath]()
		if err != nil {
			panic("Asset " + assetPath + " not found.")
		}
		tmplText := string(a.bytes)
		updated := strings.ReplaceAll(tmplText, "__PUBLIC_PATH_PREFIX__", html.EscapeString(prefix))
		a.bytes = []byte(updated)
		a.info = modifiedFileInfo{a.info, int64(len(a.bytes))}
		_bindata[assetPath] = func() (*asset, error) {
			return a, nil
		}
	}
	rewrite("build/index.html")
	rewrite("build/diagnoseReport.html")
}
