---
name: update-dashboard
description: "Update the pingcap/tidb-dashboard dependency to the latest commit from its master branch. Bumps the module in go.mod/go.sum across root, tests/integrations, and tools sub-modules, runs go mod tidy, verifies the build, and commits. Use when a new tidb-dashboard release needs to be pulled into PD."
compatibility: Requires Go toolchain and network access to proxy.golang.org. Must be on a clean working tree on master or a feature branch.
---

# Update TiDB Dashboard Dependency

## Overview

Routine maintenance skill: bump `github.com/pingcap/tidb-dashboard` to the latest commit from the `master` branch of `pingcap/tidb-dashboard`, keeping all three Go modules in sync.

## Phase 1: Preparation

1. Ensure the working tree is clean:
   ```bash
   git status --short
   ```
   If dirty, stop and ask the user to commit or stash first.

2. Confirm you are on the correct base branch (usually `master`):
   ```bash
   git branch --show-current
   ```

3. Fetch the latest upstream master:
   ```bash
   git fetch upstream master
   git rebase upstream/master
   ```

## Phase 2: Find the Latest Dashboard Version

1. Query the latest tidb-dashboard commit available on the Go module proxy:
   ```bash
   GOPROXY=https://proxy.golang.org,direct go list -m -json github.com/pingcap/tidb-dashboard@master
   ```
   Extract the `Version` field (e.g. `v0.0.0-20260316045729-aa6178a60657`).

2. Also extract the short commit hash from the version string (the part after the last `-`).

3. Confirm the new version is actually newer than the current one in `go.mod`. If already up-to-date, report that no update is needed and stop.

## Phase 3: Update All Modules

The PD repo has three `go.mod` files that reference tidb-dashboard. Update all three consistently.

1. **Root module** (`./`):
   ```bash
   go get github.com/pingcap/tidb-dashboard@<version>
   go mod tidy
   ```

2. **Integration tests** (`./tests/integrations/`):
   ```bash
   cd tests/integrations
   go get github.com/pingcap/tidb-dashboard@<version>
   go mod tidy
   cd ../..
   ```

3. **Tools** (`./tools/`):
   ```bash
   cd tools
   go get github.com/pingcap/tidb-dashboard@<version>
   go mod tidy
   cd ..
   ```

4. If the new tidb-dashboard version requires a newer Go toolchain (check for errors like `requires go >= X.Y.Z`), update the `go` directive in all three `go.mod` files accordingly.

## Phase 4: Verify

1. Build the PD server to catch compilation errors:
   ```bash
   make pd-server
   ```
   If the build fails, investigate and fix. Common causes:
   - API changes in tidb-dashboard requiring PD code updates
   - Go toolchain version mismatch

2. Verify the three `go.mod` files all reference the same tidb-dashboard version:
   ```bash
   grep 'tidb-dashboard' go.mod tests/integrations/go.mod tools/go.mod
   ```

## Phase 5: Find or Create Tracking Issue

Before committing, ensure there is a GitHub issue for this dashboard bump.

1. Search for an existing open issue:
   ```bash
   gh issue list --repo tikv/pd --search "update tidb-dashboard <short-hash>" --state open --limit 5
   ```
   Also try broader searches if the exact hash doesn't match:
   ```bash
   gh issue list --repo tikv/pd --search "bump tidb-dashboard" --state open --limit 5
   ```

2. If a matching issue exists, note its number for the PR.

3. If no matching issue exists, create one:
   ```bash
   gh issue create --repo tikv/pd \
     --title "chore(dashboard): update TiDB Dashboard to <version-timestamp>-<short-hash>" \
     --body "Routine dependency bump: update \`github.com/pingcap/tidb-dashboard\` to \`<full-version>\`."
   ```
   Note the created issue number.

## Phase 6: Commit and Create PR

1. Create a feature branch:
   ```bash
   git checkout -b chore/update-dashboard-<short-commit-hash>
   ```

2. Stage only the relevant files:
   ```bash
   git add go.mod go.sum tests/integrations/go.mod tests/integrations/go.sum tools/go.mod tools/go.sum
   ```

3. Commit with a descriptive message following PD conventions:
   ```bash
   git commit -s -m 'chore(dashboard): update TiDB Dashboard to <version-timestamp>-<short-hash> [master]'
   ```

4. Use the `create-pr` skill to push and open a PR. The PR should:
   - Link to the tracking issue found or created in Phase 5: `Close #<issue-number>`
   - Reference the upstream dashboard commit or PR if known
   - Use the `chore(dashboard):` prefix in the title
   - Note any Go toolchain version bump if applicable

## Common Pitfalls

- **Do not forget `tests/integrations` and `tools` modules.** All three must stay in sync.
- **Do not skip `go mod tidy`.** It resolves transitive dependency changes.
- **Check for Go toolchain bumps.** New dashboard versions sometimes pull in deps requiring a newer Go version.
- **Do not update other dependencies.** Keep the change scoped to tidb-dashboard only. If `go mod tidy` pulls in unrelated changes, investigate.
