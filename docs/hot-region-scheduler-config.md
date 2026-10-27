# Hot Region scheduler configuration validation

The hot Region scheduler validates configuration when it is created, updated
through its `/config` endpoint, or reloaded from storage. An invalid HTTP update
returns `400 Bad Request` and preserves both the active and persisted configuration.
An invalid reload returns an error and preserves the active configuration.

| Setting | Accepted values |
| --- | --- |
| `history-sample-duration` | Zero to disable history, or a positive duration at least as long as `history-sample-interval`. |
| `history-sample-interval` | A positive duration, including when history is disabled. |
| History sample count | `floor(history-sample-duration / history-sample-interval)` must not exceed 1024. |
| `max-peer-number` | A positive integer. |
| `max-zombie-rounds` | A nonnegative integer whose product with the Region heartbeat interval fits in `time.Duration`. Zero removes the delay in clearing pending influence after an operator ends. |
| `min-hot-byte-rate`, `min-hot-key-rate`, `min-hot-query-rate`, `min-hot-cpu-rate` | Finite, nonnegative numbers. |
| `src-tolerance-ratio`, `dst-tolerance-ratio` | Finite, nonnegative numbers. Zero retains its existing meaning of disabling expectation and variance checks. |
| `byte-rate-rank-step-ratio`, `key-rate-rank-step-ratio`, `query-rate-rank-step-ratio`, `cpu-rate-rank-step-ratio`, `count-rank-step-ratio` | Finite numbers in `(0, 1]`. |
| `great-dec-ratio`, `minor-dec-ratio` | Finite numbers in `(0, 1]`, with `great-dec-ratio <= minor-dec-ratio`. |
| `pending-weight` | A finite number of at least 1. |
| `split-thresholds` | A finite number in `[0.01, 1]`. |

The history cache also caps each dimension's sample buffer at 1024 entries.
Each buffer therefore holds at most 8 KiB of sample values. With four dimensions,
two read/write types and two resource kinds, the sample buffers total at most
128 KiB per store, excluding metadata and temporary calculation results.
