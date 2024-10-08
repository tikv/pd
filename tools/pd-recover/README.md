# pd-recover

`pd-recover` is a disaster recovery tool of PD, used to recover the PD cluster which cannot start or provide services normally.

## Build

1. [Go](https://golang.org/) Version 1.23 or later
2. In the root directory of the [PD project](https://github.com/tikv/pd), use the `make pd-recover` command to compile and generate `bin/pd-recover`.

## Usage

The details about how to use `pd-recover` can be found in [PD Recover User Guide](https://docs.pingcap.com/tidb/dev/pd-recover).
