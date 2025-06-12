# pd-simulator

pd-simulator is a tool to reproduce some scenarios and evaluate the schedulers' efficiency.

## Build

1. [Go](https://golang.org/) Version 1.23 or later
2. In the root directory of the [PD project](https://github.com/tikv/pd), use the `make simulator` command to compile and generate `bin/pd-simulator`

## Usage

This section describes how to use the simulator.

### Flags Description

```shell
-pd string
      Specify a PD address (if this parameter is not set, it will start a PD server from the simulator inside)
-config string
      Specify a configuration file for the PD simulator
-case string
      Specify the case which the simulator is going to run
-serverLogLevel string
      Specify the PD server log level (default: "fatal")
-simLogLevel string
      Specify the simulator log level (default: "fatal")
```

Run all cases:

```shell
./pd-simulator
```

Run a specific case with an internal PD:

You can check case name in `tools/pd-simulator/simulator/cases/cases.go`.

```shell
./pd-simulator -case="casename"
```

Run a specific case with an external PD:

```shell
./pd-simulator -pd="http://127.0.0.1:2379" -case="casename"
```

Run with tiup playground:
```shell
tiup playground nightly --host 127.0.0.1 --kv.binpath ./pd-simulator --kv=1 --db=0 --kv.config=./tikv.conf
```
tikv conf
```
case-name="redundant-balance-region"
sim-tick-interval="1s"
store-io-per-second=100
```
