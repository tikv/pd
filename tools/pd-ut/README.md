# pd-ut

pd-ut is a tool to run unit tests for PD.

## Build

1. [Go](https://golang.org/) Version 1.23 or later
2. In the root directory of the [PD project](https://github.com/tikv/pd), use the `make pd-ut` command to compile and generate `bin/pd-ut`

## Usage

This section describes how to use the pd-ut tool.

### brief run all tests
```shell
make ut 
```


### run by pd-ut

- You should `make failpoint-enable` before running the tests.
- And after running the tests, you should `make failpoint-disable` and `make clean-test` to disable the failpoint and clean the environment.

#### Flags description

```shell
// run all tests
pd-ut

// show usage
pd-ut -h

// list all packages
pd-ut list

// list test cases of a single package
pd-ut list $package

// list test cases that match a pattern
pd-ut list $package 'r:$regex'

// run all tests
pd-ut run

// run test all cases of a single package
pd-ut run $package

// run test cases of a single package
pd-ut run $package $test

// run test cases that match a pattern
pd-ut run $package 'r:$regex'

// build all test package
pd-ut build

// build a test package
pd-ut build xxx

// write the junitfile
pd-ut run --junitfile xxx

// test with race flag
pd-ut run --race

// test with coverprofile
pd-ut run --coverprofile xxx
go tool cover --func=xxx
```
