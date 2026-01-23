<!--

Thank you for working on PD! Please read PD's [CONTRIBUTING](https://github.com/tikv/pd/blob/master/CONTRIBUTING.md) document **BEFORE** filing this PR.

PR Title Format:
1. pkg [, pkg2, pkg3]: what's changed
2. *: what's changed

-->

### What problem does this PR solve?

<!--

Please create an issue first to describe the problem.
There MUST be one line starting with "Issue Number:  " and 
linking the relevant issues via the "close" or "ref".
For more info, check https://github.com/tikv/pd/blob/master/CONTRIBUTING.md#linking-issues.

-->
Issue Number: Close #9390

### What is changed and how does it work?

<!--

You could use the "commit message" code block to add more description to the final commit message.
For more info, check https://github.com/tikv/pd/blob/master/CONTRIBUTING.md#format-of-the-commit-message.

-->

```commit-message
server: fix io.EOF wrapping in gRPC streams (heartbeatServer, tsoServer, bucketHeartbeatServer) to ensure correct error comparison

The `bucketHeartbeatServer`, `tsoServer`, and `heartbeatServer` wrappers in `pd/server/grpc_service.go` were unconditionally wrapping errors returned by `Recv()` and `Send()` with `errors.WithStack`. This included `io.EOF`, which is a sentinel error used to signal the end of a stream.

When `io.EOF` is wrapped, standard equality checks (`err == io.EOF`) fail. Upstream callers (like `server.recv` in `ReportBuckets`) that expect to handle stream termination gracefully instead receive a generic error, potentially causing incorrect error handling flow or unnecessary noise in logs.

I modified `pd/server/grpc_service.go` to explicitly check for `io.EOF` in the error handling blocks of these methods. If `io.EOF` is detected, it is returned directly without wrapping.
```

#### Verification

I added 5 new regression test cases in `server/grpc_service_test.go` to verify correct `io.EOF` handling and state management for each affected server wrapper. These tests mock the stream to return `io.EOF` and assert that the wrapper returns it unwrapped and sets the closed state.

*   `TestBucketHeartbeatServerRecvEOF`: Verify `bucketHeartbeatServer` `Recv` behavior.
*   `TestHeartbeatServerSendEOF`: Verify `heartbeatServer` `Send` behavior.
*   `TestHeartbeatServerRecvEOF`: Verify `heartbeatServer` `Recv` behavior.
*   `TestTsoServerSendEOF`: Verify `tsoServer` `Send` behavior.
*   `TestTsoServerRecvEOF`: Verify `tsoServer` `recv` behavior.

To run the verification tests:

```bash
go test -v ./server/ -run "Test.*EOF"
```

### Check List

<!-- Remove the items that are not applicable. -->

Tests

<!-- At least one of these tests must be included. -->

- [x] Unit test
- [ ] Integration test
- [ ] Manual test (add detailed scripts or steps below)
- [ ] No code

Code changes

- [ ] Has the configuration change
- [ ] Has HTTP API interfaces changed (Don't forget to [add the declarative for the new API](https://github.com/tikv/pd/blob/master/docs/development.md#updating-api-documentation))
- [ ] Has persistent data change

Side effects

- [ ] Possible performance regression
- [ ] Increased code complexity
- [ ] Breaking backward compatibility

Related changes

- [ ] PR to update [`pingcap/docs`](https://github.com/pingcap/docs)/[`pingcap/docs-cn`](https://github.com/pingcap/docs-cn):
- [ ] PR to update [`pingcap/tiup`](https://github.com/pingcap/tiup):
- [ ] Need to cherry-pick to the release branch

### Release note

<!--

A bugfix or a new feature needs a release note. If there is no need to give a release note, just leave it with the `None`.

Please refer to [Release Notes Language Style Guide](https://pingcap.github.io/tidb-dev-guide/contribute-to-tidb/release-notes-style-guide.html) to write a quality release note.

-->

```release-note
server: fix io.EOF wrapping in gRPC streams (heartbeatServer, tsoServer, bucketHeartbeatServer) to ensure correct error comparison
```
