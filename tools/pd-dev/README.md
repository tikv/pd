# pd-dev

pd-dev is a tool to run tests for PD.

## Build

1. [Go](https://golang.org/) Version 1.21 or later
2. In the root directory of the [PD project](https://github.com/tikv/pd), use the `make pd-dev` command to compile and generate `bin/pd-dev`

## Usage

This section describes how to use the pd-dev tool.

### brief run all tests

```shell
make ut
```

### run by pd-dev

- You should `make failpoint-enable` before running the tests.
- And after running the tests, you should `make failpoint-disable` and `make clean-test` to disable the failpoint and clean the environment.

#### Flags description

```shell
// run all tests
pd-dev

// show usage
pd-dev -h

// list all packages
pd-dev list

// list test cases of a single package
pd-dev list $package

// list test cases that match a pattern
pd-dev list $package 'r:$regex'

// run all tests
pd-dev run

// run test all cases of a single package
pd-dev run $package

// run test cases of a single package
pd-dev run $package $test

// run test cases that match a pattern
pd-dev run $package 'r:$regex'

// build all test package
pd-dev build

// build a test package
pd-dev build xxx

// write the junitfile
pd-dev run --junitfile xxx

// test with race flag
pd-dev run --race

// test with coverprofile
pd-dev run --coverprofile xxx
go tool cover --func=xxx
```
