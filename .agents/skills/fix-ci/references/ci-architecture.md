# PD CI Architecture Reference

## Workflows

| Workflow | File | Trigger | Jobs |
|---|---|---|---|
| PD Test | `.github/workflows/pd-tests.yaml` | push/PR to master, release-* | 10 parallel test chunks + coverage report |
| Check PD | `.github/workflows/check.yaml` | push/PR | Build (SWAGGER=1), tools, `make check` |
| TSO Function Test | `.github/workflows/tso-function-test.yaml` | push/PR to master, release-* | `make test-tso-function` |
| Docker Image | `.github/workflows/pd-docker-image.yaml` | push/PR to master | `make docker-image` |

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
