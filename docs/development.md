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

Use `apiutil.ErrorResp` with `errs.NewInvalidInputErr` or `errs.NewInternalErr` for structured error responses.
These responses include the `TiDB-Error-Code` header and a JSON body with `code`, `msg`, and `data` fields. Internal errors also include a `stack` field.
Legacy `errcode.ErrorCode` values retain their error codes and HTTP mappings, including custom codes. The response also preserves `operation` and `others` when supplied by the error.

`make test-errcode-compatibility` checks the adapter against errcode v0.3.0 in an isolated test module. Both `make test` and `make basic-test` include this check; the main, client, tools, and integration modules do not depend on errcode.
