# pd-api-bench

pd-api-bench is a tool to test PD API.

## Build

1. [Go](https://golang.org/) Version 1.23 or later
2. In the root directory of the [PD project](https://github.com/tikv/pd), use the `make pd-api-bench` command to compile and generate `bin/pd-api-bench`

## Usage

This section describes how to use the `pd-api-bench` tool.

### Cases

The api bench cases we support are as follows:

1. HTTP

    + GetRegionStatus: /pd/api/v1/stats/region
    + GetMinResolvedTS: /pd/api/v1/min-resolved-ts

2. gRPC

    + GetRegion
    + GetStore
    + GetStores
    + ScanRegions

### Flags description

-cacert string
> path of file that contains list of trusted SSL CAs

-cert string
> path of file that contains X509 certificate in PEM format

-key string
> path of file that contains X509 key in PEM format

-client int
> the client number (default 1)

-pd string
> pd address (default "127.0.0.1:2379")

-http-cases
> HTTP api bench cases list. Multiple cases are separated by commas. Such as `-http-cases GetRegionStatus,GetMinResolvedTS`
> You can set qps with '-' and set burst with '+' such as `-http-cases GetRegionStatus-100+1`

-grpc-cases
> gRPC api bench cases list. Multiple cases are separated by commas. Such as `-grpc-cases GetRegion`
> You can set qps with '-' and set burst with '+' such as `-grpc-cases GetRegion-1000000+10`

-qps
> the qps of request (default 1000). It will set qps for all cases except those in which qps is specified separately

-burst
> the burst of request (default 1). It will set burst for all cases except those in which burst is specified separately

-debug
> print the output of api response for debug

### Run Shell

You can run shell as follows.

```shell
go run main.go --http-cases GetRegionStatus-1+1,GetMinResolvedTS-1+1 --client 1 --debug true
```

### TLS

You can use the following command to generate a certificate for testing TLS:

```shell
mkdir cert
./cert_opt.sh generate cert
go run main.go --http-cases GetRegionStatus-1+1,GetMinResolvedTS-1+1 --client 1 --debug true --cacert ./cert/ca.pem --cert ./cert/pd-server.pem  --key ./cert/pd-server-key.pem
./cert_opt.sh cleanup cert
rm -rf cert
```
