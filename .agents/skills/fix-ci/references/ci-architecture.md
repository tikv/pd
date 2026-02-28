# PD CI Architecture Reference

## Workflows

| Workflow | File | Trigger | Jobs |
|---|---|---|---|
| PD Test | `.github/workflows/pd-tests.yaml` | push/PR to master, release-* | 10 parallel test chunks + coverage report |
| Check PD | `.github/workflows/check.yaml` | push/PR | Build (SWAGGER=1), tools, `make check` |
| TSO Function Test | `.github/workflows/tso-function-test.yaml` | push/PR to master, release-* | `make test-tso-function` |
| Docker Image | `.github/workflows/pd-docker-image.yaml` | push/PR to master | `make docker-image` |

## Test Matrix (PD Test workflow — `pd-tests.yaml`)

The main test workflow uses a 10-job matrix. Each job runs `make ci-test-job JOB_INDEX=<worker_id>`, dispatched via `scripts/ci-subtask.sh`.

| Worker ID | Job Name | Scope | Packages / Directories |
|---|---|---|---|
| 1 | Unit Test(1) | Unit tests (broad) | All except `tests/`, `server/`, `pkg/schedule/`, `pkg/utils/`, `pkg/encryption/` |
| 2 | Unit Test(2) | Unit tests (schedule + encryption) | `schedule`, `encryption` (excluding `tests/`) |
| 3 | Unit Test(3) | Unit tests (server + utils) | `server`, `utils` (excluding `tests/`) |
| 4 | Tests(1) | Integration tests (broad) | `tests/` except `tests/server/api`, `tests/server/cluster`, `tests/server/config` |
| 5 | Tests(2) | Integration tests (server API/cluster/config) | `tests/server/api`, `tests/server/cluster`, `tests/server/config` |
| 6 | Tools Test | Tool tests | `tools/` subdirectory (runs via `cd ./tools && make ci-test-job`) |
| 7 | Client Integration Test | Client integration + submodule | `pd-ut it run client` + `cd ./client && make ci-test-job` |
| 8 | TSO Integration Test | TSO integration | `pd-ut it run tso` (excluding `mcs/tso`) |
| 9 | Microservice Integration(!TSO) | MCS integration (non-TSO) | `pd-ut it run mcs` (excluding `mcs/tso`) |
| 10 | Microservice Integration(TSO) | MCS TSO integration | `pd-ut it run mcs/tso` |

## Test Runner: `pd-ut`

**Unit tests:**

```bash
pd-ut run <packages> --race --coverprofile <file> --junitfile <file>
# <packages> = comma-separated list: schedule,encryption
# --ignore <packages> = exclude packages
```

**Integration tests:**

```bash
pd-ut it run <suite> --race --coverprofile <file> --junitfile <file>
# <suite> = client | tso | mcs | mcs/tso
# --ignore <packages> = exclude sub-packages
```

## Fetching Logs with `gh` CLI

```bash
# List jobs in a run
gh run view <RUN_ID> --repo tikv/pd

# Download failed job logs
gh run view <RUN_ID> --repo tikv/pd --log-failed > /tmp/ci-logs/run-<RUN_ID>-failed.log 2>&1

# Download a specific job's log
gh run view --repo tikv/pd --job <JOB_ID> --log > /tmp/ci-logs/job-<JOB_ID>.log 2>&1

# List PR checks
gh pr checks <PR_NUMBER> --repo tikv/pd

# Read issue body (to find linked CI runs)
gh issue view <ISSUE_NUMBER> --repo tikv/pd --json body,comments
```

## Mapping Job Names to Source Code Areas

When a CI job fails, use this mapping to narrow down which packages to investigate:

| Job Name Contains | Look In |
|---|---|
| `Unit Test(1)` | `pkg/` (excluding `schedule/`, `utils/`, `encryption/`, `server/`) |
| `Unit Test(2)` | `pkg/schedule/`, `pkg/encryption/` |
| `Unit Test(3)` | `server/`, `pkg/utils/` |
| `Tests(1)` | `tests/` (excluding `tests/server/api`, `tests/server/cluster`, `tests/server/config`) |
| `Tests(2)` | `tests/server/api/`, `tests/server/cluster/`, `tests/server/config/` |
| `Tools Test` | `tools/` |
| `Client Integration` | `tests/integrations/client/`, `client/` |
| `TSO Integration` | `tests/integrations/tso/` |
| `Microservice Integration(!TSO)` | `tests/integrations/mcs/` (excluding `mcs/tso`) |
| `Microservice Integration(TSO)` | `tests/integrations/mcs/tso/` |
| `Check PD` / `statics` | Build/lint issue — check formatting, imports, `go.mod` |
| `TSO Function Test` | `pkg/tso/`, `server/` (TSO-related code) |
