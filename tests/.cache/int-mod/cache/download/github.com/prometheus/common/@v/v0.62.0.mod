module github.com/prometheus/common

go 1.21

require (
	github.com/alecthomas/kingpin/v2 v2.4.0
	github.com/google/go-cmp v0.6.0
	github.com/julienschmidt/httprouter v1.3.0
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f
	github.com/prometheus/client_model v0.6.1
	github.com/stretchr/testify v1.10.0
	golang.org/x/net v0.33.0
	golang.org/x/oauth2 v0.24.0
	google.golang.org/protobuf v1.36.1
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.20.4 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	github.com/xhit/go-str2duration/v2 v2.1.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract v0.50.0 // Critical bug in counter suffixes, please read issue https://github.com/prometheus/common/issues/605
