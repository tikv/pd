# WatchGCStates Blocked Send Cancellation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `WatchGCStates` return and release its concurrency-limit token when its watcher terminates, even while gRPC flow control blocks a response send, and verify that the sending goroutine exits after RPC teardown.

**Architecture:** Run the existing receive/convert/send loop in one worker goroutine per RPC. The handler supervises the worker through a buffered result channel and the watcher's termination channel. The handler returns on watcher termination; gRPC then tears down the transport stream and unblocks the worker.

**Tech Stack:** Go 1.25+, the repository-pinned grpc-go v1.82.1, existing PD GC watchers, failpoint-aware Go tests, and `bufconn` for transport coverage.

**Spec:** [WatchGCStates server design](../specs/2026-09-03-watch-gc-states-design.md), especially “Lifecycle and errors” and “GC service”, refined by the decisions below. This document is the implementation handoff for the second review finding on PR #11264. The first finding, initial/live ordering, was fixed in `65c7ca00b`; preserve that fix, including `pendingLiveCount`.

**Status:** Implemented and verified against base `65c7ca00b`. Both fake-send and real-transport regressions failed against the original synchronous handler, then passed with the fix. Targeted race tests passed in `pkg/gc`, `server`, and `tests/server/gc`; the real-transport regression passed 20 repeated race runs, and affected-package basic tests passed. Independent review found no blocking issues. A full `make check` attempt passed module tidying and formatting but stopped because `golangci-lint` is unavailable in the local environment; the remaining checks still need CI or a fully provisioned environment. The task descriptions below retain the implementation and acceptance instructions for reference.

## Global constraints

- Limit production changes to `WatchGCStates` and its watcher termination notification. Other streaming RPCs, generic gRPC infrastructure, protobuf, and dependency versions are outside this task.
- Keep `rateLimitCheck()` and both deferred cleanup operations in the public `WatchGCStates` handler. Its caller-derived rate-limit label must remain `WatchGCStates`.
- Keep one receiving/sending worker per RPC and at most one `Send` in progress. Keep all existing per-response terminal checks, response-size limits, ordering, queue capacities, and manager lock scopes.
- Add no per-send goroutine, timer, response queue, or general send timeout. An open stream without a watcher termination event remains governed by the existing policy.
- The worker result channel has capacity **1**. The handler returns without joining the worker; RPC teardown supplies the cancellation needed to finish a blocked `Send`.
- Preserve error mappings and watcher termination metrics. Use `Unavailable` for leader loss, `ResourceExhausted` for slow consumers, and context status codes for caller cancellation/deadline. Preserve a raw send/conversion error when no watcher terminal cause has been recorded.
- Follow repository `AGENTS.md`. Run tests through failpoint-aware targets and restore failpoints before editing, reviewing diffs, or committing. If delegating, one owner coordinates test runs and edits in this shared worktree.

## Decision and lifetime ownership

`GCStateWatcher` owns a child context derived from `stream.Context()`. Canceling that child wakes `RecvBatch`, but does not cancel the parent transport context observed by gRPC `Send`. The existing synchronous handler cannot reach its next terminal check while sending is blocked.

The chosen design makes the handler independently responsive to watcher termination:

```text
watcher terminates
    -> supervisor returns the mapped terminal cause
    -> public handler runs watcher.Close() and releases its rate-limit token
    -> gRPC processes the handler return and closes the transport stream
    -> blocked Send returns
    -> worker publishes its result into the buffered channel and exits
```

The worker may briefly outlive the handler. It retains the response it is sending until `Send` returns; no response is mutated or recycled during that interval. The worker must not wait for a consumer of its final result. Waiting for the worker inside a handler defer would reverse this dependency and deadlock the teardown path.

The public handler remains the owner of watcher cleanup. The supervisor does not close the watcher itself. For the real caller, the watcher is always derived from the RPC context, so its `Done` notification also covers client cancellation. There is no need for a third, independently managed cancellation context or a redundant `stream.Context().Done()` select arm.

When worker completion races with watcher cancellation, prefer an already-recorded watcher cause when choosing the return value. This makes a transport cancellation caused by server teardown less likely to obscure the domain reason. It does not establish a total order for events that have not yet been recorded; the watcher continues to own its first cancellation cause.

### Verified transport assumption

In the pinned grpc-go source, `serverStream.SendMsg` calls the transport write path, whose `writeQuota.get` waits on quota or the transport stream's cancellation. `Server.processStreamingRPC` handles the application handler's return via `WriteStatus`, and `http2Server.finishStream` cancels the transport stream before queuing its final trailers. Thus handler return can release the blocked send without waiting for the client to start consuming responses.

Recheck these functions in the pinned module if its version changes:

- `google.golang.org/grpc/stream.go`: `serverStream.SendMsg`.
- `google.golang.org/grpc/server.go`: `Server.processStreamingRPC`.
- `google.golang.org/grpc/internal/transport/http2_server.go`: `writeStatus`, `finishStream`, and `write`.
- `google.golang.org/grpc/internal/transport/flowcontrol.go`: `writeQuota.get`.
- `google.golang.org/grpc/internal/transport/controlbuf.go`: `loopyWriter.processData`, which replenishes write quota only for bytes allowed by stream flow control.

This guarantees a local cancellation path, not immediate delivery of the final status to a client that refuses to read. Queued response data/trailers may still require client progress. Test handler return and worker exit before resuming client reads; inspect the client status afterward.

## Files and interfaces

| File | Responsibility |
| --- | --- |
| `pkg/gc/gc_state_watcher.go` | Add `Done() <-chan struct{}` exposing the existing watcher context's termination channel. |
| `pkg/gc/gc_state_watcher_test.go` | Verify notification, cause visibility, and existing lifecycle behavior. |
| `server/gc_service.go` | Add `Done` to `gcStateChangeReceiver`; make `serveWatchGCStates` the supervisor and move its existing loop unchanged into `sendWatchGCStates`. |
| `server/gc_service_test.go` | Adapt existing receiver fakes; add deterministic cancellation tests and real gRPC teardown coverage. |
| `tests/server/gc/gc_test.go` | Verify public-handler registration, cleanup, status, and concurrency-token release during blocked sending. |
| `docs/superpowers/specs/2026-09-03-watch-gc-states-design.md` | Document worker/supervisor ownership and distinguish unrecalled sends from handler lifetime. |

`GCStateWatcher.Done` is concurrency-safe and returns the same channel on every call. Once that channel is closed, `Err()` returns the terminal cause. Reading `Done` must not allocate a goroutine, acquire a manager lock, or register a new callback.

## Task 1: Add cancellation supervision and deterministic regressions

**Interfaces produced:** `(*gc.GCStateWatcher).Done() <-chan struct{}`, the extended `gcStateChangeReceiver`, and the internal synchronous worker `sendWatchGCStates(receiver gcStateChangeReceiver, stream pdpb.PD_WatchGCStatesServer, maxResponseSize int) error`.

- [ ] Add a context-backed receiver fake for cancellation tests. Use `context.WithCancelCause(streamCtx)` and return its `Done`/`Cause` from the fake; keep mutable batch state exclusively in the receiving worker. Existing non-cancellation fakes may return a nil `Done` channel, which disables that select arm. Never write the existing fake's plain `terminalErr` concurrently with the supervisor reading it.

  A sufficient receiver for the blocked-send tests is:

  ```go
  type cancelableGCStateReceiver struct {
      ctx     context.Context
      changes []gc.GCStateChange
  }

  func (r *cancelableGCStateReceiver) Done() <-chan struct{} { return r.ctx.Done() }
  func (r *cancelableGCStateReceiver) Err() error { return context.Cause(r.ctx) }

  func (r *cancelableGCStateReceiver) RecvBatch(maxChanges int) ([]gc.GCStateChange, error) {
      if err := r.Err(); err != nil {
          return nil, err
      }
      if len(r.changes) == 0 {
          <-r.Done()
          return nil, r.Err()
      }
      n := min(maxChanges, len(r.changes))
      batch := r.changes[:n]
      r.changes = r.changes[n:]
      return batch, nil
  }
  ```

- [ ] Write `TestServeWatchGCStatesCancellationUnblocksHandler` as table-driven cases for `errs.ErrNotLeader`/`Unavailable` and `errs.ErrGCStateWatcherSlowConsumer`/`ResourceExhausted`. For each case, create one upsert and use `fakeWatchGCStatesServer.sendHook` with the following behavior:

  ```go
  sendStarted := make(chan struct{})
  sendExited := make(chan struct{})
  stream.sendHook = func(*pdpb.WatchGCStatesResponse) error {
      close(sendStarted)
      defer close(sendExited)
      <-streamCtx.Done()
      return streamCtx.Err()
  }
  handlerDone := make(chan error, 1)
  go func() { handlerDone <- serveWatchGCStates(receiver, stream, 1024) }()
  ```

  Wait for `sendStarted`, cancel only the receiver with the table's cause, then require `handlerDone` to return the expected status within **5 seconds**, while `streamCtx.Err()` remains nil and `sendExited` remains open. Only afterward call the stream cancel function to model gRPC teardown, and require `sendExited` to close. Register failure cleanup before starting the goroutine: cancel both contexts and release/wait for test goroutines with bounded waits even if an assertion fails. This fake deliberately models the transport cancellation boundary rather than pretending watcher cancellation directly ends `Send`.

- [ ] Run the new test against the synchronous implementation and record the expected failure: the handler fails to return before the stream is canceled. The fake's extra `Done` method does not require changing the old production interface to compile this red test. Do not accept a test that first cancels the client/stream and only then checks handler completion.

- [ ] Implement the watcher notification and supervisor as follows, retaining the public handler's existing `defer watcher.Close()` and `defer done()`:

  ```go
  // Done returns a channel that is closed when the watcher terminates.
  func (w *GCStateWatcher) Done() <-chan struct{} {
      return w.ctx.Done()
  }

  type gcStateChangeReceiver interface {
      RecvBatch(maxChanges int) ([]gc.GCStateChange, error)
      Done() <-chan struct{}
      Err() error
  }

  func serveWatchGCStates(receiver gcStateChangeReceiver, stream pdpb.PD_WatchGCStatesServer, maxResponseSize int) error {
      if err := receiver.Err(); err != nil {
          return watchGCStatesErrorToStatus(err)
      }
      resultCh := make(chan error, 1)
      go func() {
          defer logutil.LogPanic()
          resultCh <- sendWatchGCStates(receiver, stream, maxResponseSize)
      }()
      select {
      case err := <-resultCh:
          if cause := receiver.Err(); cause != nil {
              return watchGCStatesErrorToStatus(cause)
          }
          return err
      case <-receiver.Done():
          return watchGCStatesErrorToStatus(receiver.Err())
      }
  }
  ```

  Import the repository's `pkg/utils/logutil`. Rename the old `serveWatchGCStates` body to `sendWatchGCStates` without changing its receive loop, conversion, splitting, per-response `Err` check, or raw error returns. `logutil.LogPanic` follows the repository's goroutine convention; it logs fatally rather than silently recovering and stranding the supervisor.

- [ ] Add the following focused assertions alongside the regression, then run them under the race detector:

  | Case | Required observation |
  | --- | --- |
  | Watcher terminates before serving starts | Mapped error returns and the fake stream's send hook is never called. |
  | Cancellation while waiting in `RecvBatch` | Handler and worker finish; no response is sent. |
  | Parent stream context canceled | Watcher notification wakes the supervisor; cancellation status is preserved. |
  | Worker send fails with no watcher cause | Existing `TestServeWatchGCStatesReturnsRawSendError` still returns the same error. |
  | Invalid internal change | Existing `Internal` mapping remains unchanged. |
  | Cancellation between split responses | Existing per-send terminal check prevents sending the remaining response. |
  | Watcher `Done` notification | Notification closes on cancellation; `Err` exposes the first cause and repeated `Done` calls return the same channel. |

  The process-level goroutine leak checker must pass. Tests must wait for their own cleanup and worker-visible exit conditions; do not reuse fake state while a worker may still access it.

**Completion:** The blocked-send red test passes without canceling its stream first, existing server adapter tests pass under `-race`, and the diff contains one worker/channel per RPC with no manager lock changes.

## Task 2: Verify real transport teardown and public-handler cleanup

**Interfaces consumed:** The Task 1 supervisor and existing public `WatchGCStates` handler. Test doubles must honor the receiver batch bound and first-cause contract.

- [ ] Add `TestWatchGCStatesTransportCancellationUnblocksSend` in `server/gc_service_test.go`. Reuse the `bufconn` setup pattern in `server/grpc_service_test.go`. Register a test service embedding `pdpb.UnimplementedPDServer`; its `WatchGCStates` implementation creates a receiver derived from the real `stream.Context()`, invokes `serveWatchGCStates`, reports the returned error through a buffered test channel, and returns it to gRPC. Supply at least **16 batches of 1024 complete upserts** with large timestamp values and distinct keyspace IDs so the generated stream exceeds **256 KiB**; have `RecvBatch` wait on its context after exhausting the fixture.

  Configure the client with:

  ```go
  grpc.WithStaticStreamWindowSize(64 << 10),
  grpc.WithStaticConnWindowSize(64 << 10),
  ```

  Use the normal uncompressed protobuf codec and a `bufconn` listener capacity of **1 MiB**. Keep the client connection and RPC context alive, and do not call client `Recv` before triggering server-side watcher cancellation. A **30-second** RPC deadline is a failure guard, not the cancellation mechanism under test.

- [ ] Make the blocked-send observation deterministic using a test-only wrapper around the real `PD_WatchGCStatesServer`. grpc-go v1.82.1 starts with a **64 KiB** stream write quota (`internal/transport/defaults.go`, `defaultWriteQuota`). With a static **64 KiB** client receive window and no application reads, cumulative successful send enqueues can exhaust that quota plus the receive window. Track successful wire bytes using `response.Size() + 5` for the gRPC message envelope:

  ```go
  func (s *observedWatchGCStatesStream) Send(response *pdpb.WatchGCStatesResponse) error {
      // These test-only constants match the pinned grpc-go implementation
      // and the explicitly configured client receive window.
      const receiveWindow = 64 << 10
      const writeQuota = 64 << 10
      if !s.observed && s.sentWireBytes >= receiveWindow+writeQuota {
          s.observed = true
          close(s.blockedSendStarted)
          defer close(s.blockedSendExited)
      }
      err := s.PD_WatchGCStatesServer.Send(response)
      if err == nil {
          s.sentWireBytes += response.Size() + 5
      }
      return err
  }
  ```

  Define the wrapper with an embedded `pdpb.PD_WatchGCStatesServer`, `sentWireBytes int`, `observed bool`, and the two notification channels. Only the sending worker accesses its counter and flag. The next send after that cumulative threshold cannot regain quota while the client does not read. The marker fires immediately before that send, which also covers cancellation racing with entry into `Send`; the original synchronous implementation still cannot observe watcher cancellation there. Recheck the byte/quota argument if the transport version or compression settings change; do not replace this observation with a sleep or a fake blocked `Send`.

- [ ] After the marker, cancel only the receiver with the domain cause. Require the test service's handler result and `blockedSendExited` within **5 seconds**, without canceling the client or stopping the gRPC server to make those assertions succeed. Then drain client responses to the terminal status and assert the expected code. Cover leader-loss and slow-consumer causes. Register unconditional cleanup so failed assertions close the client/server and release goroutines. Verify that the test fails against the old synchronous supervisor and passes after Task 1; run the focused transport test repeatedly with `-race`.

- [ ] Extend public-handler tests in `tests/server/gc/gc_test.go` using the existing `TestWatchGCStatesSendFailureCleansUpPublicHandler` setup. Enable concurrency limit **1**, observe registration, and block a test stream's `Send` on its own context. For leader loss, supersede the local manager generation with `stop := manager.OnNodeBecomesLeader()` and register `stop` for cleanup; this is a controlled domain transition, while the existing real leader-transfer test remains part of regression coverage. For slow consumption, once `Send` is blocked, produce **1025** subsequent successful increasing txn-safe-point updates to overflow the default **1024** live queue. Use the null keyspace and increasing targets so writes are neither rejected nor no-ops. If needed, factor the existing test setup into a helper instead of duplicating all rate-limit configuration code.

  Before releasing the fake transport, require: the public handler returns the domain status, `pd_gc_watcher_count` returns to its baseline, the appropriate termination counter increases once, and `GetConcurrencyLimiterStatus("WatchGCStates")` reports **0** current streams. Confirm a subsequent registered watch is admitted. On the leader-generation test, the newly superseded generation remains active until cleanup, so this admission check can use the same one-node fixture. Only then cancel the fake stream context, wait for its blocked send to exit, and clean up the subsequent watch. The fake's context cancellation models the real teardown proved by the preceding transport test.

**Completion:** Both domain causes return the expected status and free the limit token before a stalled client resumes, and the real transport test proves the blocked send exits after handler return. Existing real leader transfer and send-error cleanup tests still pass. No goroutine leak is hidden by premature client/server shutdown.

## Task 3: Update the design and run final verification

- [ ] Update the existing design document's “GC service” and “Lifecycle and errors” sections to describe the supervisor/worker split, the one-worker/one-result-channel cost, cleanup ownership, and the prohibition on joining the worker before returning. Replace the ambiguous in-progress-send sentence with this contract:

  > A response send already in progress cannot be recalled. Watcher termination nevertheless causes the RPC handler to return without waiting for that send. Handler return initiates gRPC transport teardown, which interrupts blocked sending; the worker then exits. The handler retains ownership of watcher cleanup and rate-limit token release. A client that is not reading may observe the terminal status only after draining already queued responses.

- [ ] Format touched Go files, run `git diff --check`, and execute the narrow tests before broader validation:

  ```sh
  make gotest GOTEST_ARGS='./pkg/gc ./server -run "TestGCStateWatcher|TestServeWatchGCStates|TestWatchGCStatesTransport" -count=1 -tags=without_dashboard,deadlock -race -timeout=5m'
  make gotest GOTEST_ARGS='./server -run TestWatchGCStatesTransportCancellationUnblocksSend -count=20 -tags=without_dashboard,deadlock -race -timeout=5m'
  make gotest GOTEST_ARGS='./tests/server/gc -run TestWatchGCStates -count=1 -tags=without_dashboard,deadlock -race -timeout=10m'
  GOFLAGS='-tags=without_dashboard' make basic-test BASIC_TEST_PKGS='./pkg/gc ./server'
  ```

  In this worktree, the tools required by these targets are already installed; `make -o install-tools ...` can reuse them. The `without_dashboard` tag avoids dependence on generated Dashboard assets for the integration tests. Keep regexes free of unescaped trailing `$` inside `GOTEST_ARGS`: Make can interpret the following character as a variable reference and break the shell quoting. If a wrapper fails before its cleanup branch runs, immediately run `make failpoint-disable` (or the installed-tool equivalent) before further work.

- [ ] Review the final diff against every global constraint and the acceptance cases above. Run required repository checks before PR submission, including `make check`. Confirm failpoint-generated files are absent and only intended source/test/documentation changes remain. Hand back the changed files, red/green evidence, real-transport evidence, and any unresolved failure; do not describe fake-send coverage as proof of transport worker cleanup.

**Completion:** Review and verification are complete, the handoff report distinguishes local handler exit from client-visible status delivery, and the implementation introduces no per-response concurrency or broad streaming-RPC refactor.
