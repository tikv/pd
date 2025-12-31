# Coding flow

## Building

You can build your changes with

    make

## Linting

Run linters as you make your changes.
We can recommend using VSCode with the Go addon to have this work automatically.

Official lints are run with:

    make check

This will install linter tools local to the project.
Linter versions are changed with [tools.go](../tools.go).

## Testing

The full test suite is run with:

    make test

This takes a while to run. The test suite uses a fork of [gocheck](http://labix.org/gocheck). With gocheck, individual tests can be run with this form:

    go test github.com/tikv/pd/server/api -check.f TestJsonRespondError

# Changing APIs

## Updating API documentation

We use [Swagger 2.0](https://swagger.io/specification/v2/) to automatically generate RESTful API documentation. When a PR involves API changes, you need to update the Go annotations, and the specific format can refer to [Declarative Comments Format](https://github.com/swaggo/swag#declarative-comments-format).

## Error responses

Error responses from the server are switching to using [errcode codes](https://github.com/pingcap/errcode).
They should use the `errorResp` function. Please look at other places in the codebase that use `errorResp`.
