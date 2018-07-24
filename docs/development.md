# Coding flow

## Building

You can build your changes with

    make

## Linting

Run linters as you make your changes.
We can recommend using VSCode with the Go addon to have this work automatically.

Official lints are ran with:

    make check


## Testing

The full test suite is ran with:

    make test

This takes a while to run. The test suite uses a fork of [gocheck](http://labix.org/gocheck). With gocheck, individual tests can be ran with this form:

    go test github.com/pingcap/pd/server/api -check.f TestJsonRespondError
