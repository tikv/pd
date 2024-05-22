# pd-dev

pd-dev is a tool for developing and testing PD. It provides the following functions:

- [pd-api-bench](./pd-api-bench/README.md): A tool for testing the performance of PD's API.
- [pd-tso-bench](./pd-tso-bench/README.md): A tool for testing the performance of PD's TSO.
- [pd-heartbeat-bench](./pd-heartbeat-bench/README.md): A tool for testing the performance of PD's heartbeat.
- [pd-simulator](./pd-simulator/README.md): A tool for simulating the PD cluster.
- [regions-dump](./regions-dump/README.md): A tool for dumping the region information of the PD cluster.
- [stores-dump](./stores-dump/README.md): A tool for dumping the store information of the PD cluster.

## Build

1. [Go](https://golang.org/) Version 1.21 or later
2. In the root directory of the [PD project](https://github.com/tikv/pd), use the `make pd-dev` command to compile and generate `bin/pd-dev`

## Usage

This section describes how to use the `pd-dev` tool.

### Cases

Please read related README files for more details, we support the following cases:

`./pd-dev --mode api`

`./pd-dev --mode tso`

`./pd-dev --mode heartbeat`

`./pd-dev --mode simulator`

`./pd-dev --mode regions-dump`

`./pd-dev --mode stores-dump`

`./pd-dev --mode analysis`

`./pd-dev --mode backup`

`./pd-dev --mode ut`

### flag description

--pd string
> pd address (default "127.0.0.1:2379")

--cacert string
> path of file that contains list of trusted SSL CAs

--cert string
> path of file that contains X509 certificate in PEM format

--key string
> path of file that contains X509 key in PEM format

--config string
> path of configuration file

### TLS

You can use the following command to generate a certificate for testing TLS:

```shell
mkdir cert
./cert_opt.sh generate cert
./bin/pd-dev --mode api -http-cases GetRegionStatus-1+1,GetMinResolvedTS-1+1 -client 1 -debug true -cacert ./cert/ca.pem -cert ./cert/pd-server.pem  -key ./cert/pd-server-key.pem
./cert_opt.sh cleanup cert
rm -rf cert
```
