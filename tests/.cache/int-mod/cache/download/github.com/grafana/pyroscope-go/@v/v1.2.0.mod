module github.com/grafana/pyroscope-go

go 1.17

replace github.com/grafana/pyroscope-go/godeltaprof => ./godeltaprof

require (
	github.com/grafana/pyroscope-go/godeltaprof v0.1.8
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/klauspost/compress v1.17.8 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
