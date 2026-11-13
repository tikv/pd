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
Errors implementing `errs.HTTPError` and existing `errcode.ErrorCode` values retain their code and HTTP mapping. Ordinary errors return HTTP 500 with the error message as a JSON string. Keep existing HTTP status codes and error messages stable: clients use them for retry decisions, fallback behavior, and diagnostics.

Run `make test-errcode-compatibility` to check legacy callers against the real library in an isolated test module. The root, tools, and integrations modules do not depend on it.
