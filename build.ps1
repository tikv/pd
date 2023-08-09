# For `--version`
$PD_PKG = "github.com/tikv/pd"
$GO_LDFLAGS = "-X `"$PD_PKG/server.PDReleaseVersion=$(git describe --tags --dirty --always)`""
$GO_LDFLAGS += " -X `"$PD_PKG/server.PDBuildTS=$(date -u '+%Y-%m-%d_%I:%M:%S')`""
$GO_LDFLAGS += " -X `"$PD_PKG/server.PDGitHash=$(git rev-parse HEAD)`""
$GO_LDFLAGS += " -X `"$PD_PKG/server.PDGitBranch=$(git rev-parse --abbrev-ref HEAD)`""

# Download Dashboard UI
powershell.exe -File ./scripts/embed-dashboard-ui.ps1

# Output binaries
go build -ldflags $GO_LDFLAGS -o bin/pd-server.exe cmd/pd-server/main.go
echo "bin/pd-server.exe"
go build -ldflags $GO_LDFLAGS -o bin/pd-ctl.exe tools/pd-ctl/main.go
echo "bin/pd-ctl.exe"
go build -o bin/pd-tso-bench.exe tools/pd-tso-bench/main.go
echo "bin/pd-tso-bench.exe"
go build -o bin/pd-recover.exe tools/pd-recover/main.go
echo "bin/pd-recover.exe"
