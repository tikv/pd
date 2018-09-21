pd-tso-bench
========

pd-tso-bench is a tool to benchmark GetTS performance.

## Build
1. [Go](https://golang.org/) Version 1.9 or later
2. In the root directory of the [PD project](https://github.com/pingcap/pd), use the `make` command to compile and generate `bin/pd-tso-bench`


## Usage

This section describes how to benchmark the GetTS performance.

### Flags description

```
-pd string
      Specify a PD address (default: "http://127.0.0.1:2379")
-C int
      Specify the concurrency (default: "1000")
-interval duration
      Specify the interval to output the statistics (default: "1s")
-cacert string
      Specify the path to the trusted CA certificate file in PEM format
-cert string
      Specify the path to the SSL certificate file in PEM format
-key string
      Specify the path to the SSL certificate key file in PEM format, which is the private key of the certificate specified by `--cert`
```

Benchmark the GetTS performance:

    ./pd-tso-bench

It will print some benchmark results like:
```bash
count:606148, max:9, min:0, >1ms:487565, >2ms:108403, >5ms:902, >10ms:0, >30ms:0
count:714375, max:5, min:0, >1ms:690071, >2ms:13864, >5ms:1, >10ms:0, >30ms:0
count:634645, max:6, min:0, >1ms:528354, >2ms:98148, >5ms:46, >10ms:0, >30ms:0
count:565745, max:10, min:0, >1ms:420304, >2ms:135403, >5ms:3792, >10ms:1, >30ms:0
count:583051, max:11, min:0, >1ms:439761, >2ms:135822, >5ms:1657, >10ms:1, >30ms:0
count:630377, max:6, min:0, >1ms:526209, >2ms:95165, >5ms:396, >10ms:0, >30ms:0
count:688006, max:4, min:0, >1ms:626094, >2ms:49262, >5ms:0, >10ms:0, >30ms:0
...
```