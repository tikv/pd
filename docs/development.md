# Coding flow

## Building

You can build your changes with

    make

## Linting

Run linters as you make your changes.
We can recommend using VSCode with the Go addon to have this work automatically.

Official lints are ran with:

    make check

This will use `go get` to install `retool` which then vendors the linter tools local to the project.

## Testing

The full test suite is ran with:

    make test

This takes a while to run. The test suite uses a fork of [gocheck](http://labix.org/gocheck). With gocheck, individual tests can be ran with this form:

    go test github.com/pingcap/pd/server/api -check.f TestJsonRespondError

# Changing APIs

## Updating API documentation

We use [RAML 1.0](https://github.com/raml-org/raml-spec/blob/master/versions/raml-10/raml-10.md) to manage the API documentation, and the raml file is placed in `server/api/api.raml`. We also use [raml2html](https://github.com/raml2html/raml2html) to generate a more readable html file, which is placed in `docs/api.html`. When a PR involves API changes, you need to update the raml file within the same PR.

Consider that raml2html depends on various npm packages and can only be run under a specific version of node. It is recommended to use docker to simplify the compilation of raml. You can run it in the root directory of PD:

    docker pull mattjtodd/raml2html:latest
    docker run --rm -v $PWD:/raml mattjtodd/raml2html -i /raml/server/api/api.raml -o /raml/docs/api.html


## Error responses

Error responses from the server are switching to using [error codes](./pkg/error_code/error_code.go).
The should use the `errorResp` function. Please look at other places in the codebase that use `errorResp`.
